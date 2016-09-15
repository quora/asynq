# Copyright 2016 Quora, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import inspect
import six
import sys

import qcore.helpers as core_helpers
import qcore.inspection as core_inspection
import qcore.errors as core_errors

from . import debug
from . import futures
from . import scheduler
from . import _debug

__traceback_hide__ = True

_debug_options = _debug.options
_futures_none = futures._none
_none_future = futures.none_future

MAX_DUMP_INDENT = 40


class AsyncTaskCancelledError(GeneratorExit):
    pass


class AsyncTaskResult(GeneratorExit):
    def __init__(self, result):
        GeneratorExit.__init__(self, "AsyncTaskResult")
        self.result = result


class AsyncTask(futures.FutureBase):
    """Asynchronous task implementation.
    Uses passed generator (normally produced by generator method)
    to implement asynchronous "await" behavior via 'yield' keyword.

    """

    def __init__(self, generator, fn, args, kwargs, group_cancel=True, max_depth=0):
        super(AsyncTask, self).__init__()
        if _debug_options.ENABLE_COMPLEX_ASSERTIONS:
            assert core_inspection.is_cython_or_generator(generator), \
                'generator is expected, but %s is provided' % generator
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.is_scheduled = False
        self.iteration_index = 0
        self.group_cancel = group_cancel
        self.caller = None
        self.depth = 0
        self.max_depth = max_depth
        self.scheduler = None
        self.creator = scheduler.get_active_task()
        self._generator = generator
        self._frame_info = None
        self._last_value = None
        self._dependencies = set()
        if self.creator is None:
            self._contexts = []
        else:
            self._contexts = list(self.creator._contexts)
        if _debug_options.DUMP_NEW_TASKS:
            debug.write('@async: new task: %s, created by %s' %
                (debug.str(self), debug.str(self.creator)))

    def can_continue(self):
        """Indicates whether this async task has more steps to execute.

        Task can't continue, if its underlying generator is disposed.

        """
        return self._generator is not None

    def is_blocked(self):
        """Indicates whether this async task is currently blocked.
        Blocked tasks are tasks waiting for completion of other
        tasks or batches.

        """
        return True if self._dependencies else False

    def start(self, run_immediately=False):
        """
        Starts the task on the current task scheduler.
        Newly created tasks aren't started by default.

        :param run_immediately: indicates whether first iteration
            over the generator must be performed immediately
        :return: self

        """
        s = scheduler.get_scheduler()
        return s.start(self, run_immediately)

    def after(self, future):
        """Starts the task after specified future gets computed.

        :param future: future that must be computed
        :return: self

        """
        s = scheduler.get_scheduler()
        return s.continue_with(future, self)

    def cancel_dependencies(self, error):
        if not self._dependencies:
            return
        dependencies = list(self._dependencies)
        cancellation_error = error if isinstance(error, AsyncTaskCancelledError) else AsyncTaskCancelledError(error)
        if _debug_options.DUMP_DEPENDENCIES:
            debug.write('@async: -> cancelling dependencies of %s:' % debug.str(self))
        for dependency in dependencies:
            _cancel_futures(dependency, cancellation_error)
        if _debug_options.DUMP_DEPENDENCIES:
            debug.write('@async: <- cancelled dependencies of %s' % debug.str(self))

    def _compute(self):
        # Forwards the call to task scheduler
        scheduler.get_scheduler().await([self])
        # No need to assign a value/error here, since
        # _continue method (called by TaskScheduler) does this.

    def _computed(self):
        try:
            if self._generator is not None:
                self._generator.close()
                self._generator = None
        finally:
            # super() doesn't work in Cython-ed version here
            futures.FutureBase._computed(self)
            error = self.error()
            if error is not None and self.group_cancel:
                self.cancel_dependencies(error)

    def _continue(self):
        self._before_continue()
        # Obvious optimization: we don't need to return from
        # this method, if we still can go further after yield.
        # So we return only if there are dependencies or
        # current task is computed.
        while True:
            try:
                value = unwrap(self._last_value)
                error = None
            except BaseException as e:
                value = None
                error = e
            try:
                self._accept_yield_result(self._continue_on_generator(value, error))
            except StopIteration as error:  # Most frequent, so it's the first one
                if hasattr(error, 'value'):
                    # We're on a Python version that supports adding a value to StopIteration
                    self._queue_exit(error.value)
                else:
                    # This means there was no asynq.result() call, so the value of
                    # this task should be None
                    self._queue_exit(None)
            except GeneratorExit as error:
                error_type = type(error)
                if error_type is AsyncTaskResult:
                    self._queue_exit(error.result)
                elif error_type is AsyncTaskCancelledError:
                    self._accept_error(error)
                else:
                    self._queue_exit(None)
            except BaseException as error:
                self._accept_error(error)
            finally:
                if self._dependencies or self.is_computed():
                    self._after_continue()
                    return

    def _continue_on_generator(self, value, error):
        try:
            if error is None:
                if self._generator is None:
                    raise StopIteration()
            elif self._generator is None:
                raise error

            self.iteration_index += 1
            if error is None:
                return self._generator.send(value)
            else:
                self._last_value = None  # No need to keep it further
                self._frame_info = debug.get_frame_info(self._generator)
                if hasattr(error, '_task'):
                    return self._generator.throw(error._type_, error, error._traceback)
                else:
                    return self._generator.throw(type(error), error)
        except (StopIteration, GeneratorExit):
            # Returning leads to a StopIteration exception, which is
            # handled here. In this case we shouldn't need to extract frame
            # info, because that is slow.
            self._generator = None
            raise
        except:
            # Any exception means generator is closed at this point,
            # since it fall through all exception handlers there;
            # "return" leads to StopIteration exception, so it's
            # handled here as well.
            # If the task failed, we want to save the frame info here so that the traceback can
            # show where in the async task the failure happened. However, if the error was thrown
            # into the generator, we'll already have set the frame info.
            if self._frame_info is None:
                tb = sys.exc_info()[2]

                while tb.tb_next is not None:
                    tb = tb.tb_next
                self._frame_info = inspect.getframeinfo(tb.tb_frame)
            self._generator = None
            raise

    def _accept_yield_result(self, result):
        if _debug_options.DUMP_YIELD_RESULTS:
            debug.write('@async: yield: %s -> %s' % (debug.str(self), debug.repr(result)))
        self._last_value = result
        self.scheduler.add_dependencies(self, result)

    def _accept_error(self, error):
        if self._value is not _futures_none:  # is_computed(), but faster
            # We can't change the value in this case, so we can just return here.
            pass
        else:
            # when there is no _task it means that this is the bottommost level of the async
            # task. We must attach the traceback as soon as possible
            if not hasattr(error, '_task'):
                error._task = self
                core_errors.prepare_for_reraise(error)
            else:
                # when we already have the _task on the error, it means that
                # some child generator of ours had an error.
                # now, we are storing the _traceback on the error. we use this so we can
                # raise it with that exact traceback later.
                # now, we when do raise it, the upper level gets a new traceback
                # with the curent level's traceback connected via a linked list pointer.
                # known as tb_next in traceback object.
                # this is really important. if we keep updating this traceback,
                # we can glue all the different tasks' tracebacks and make it look like
                # the error came from there.
                error._traceback = sys.exc_info()[2]

            if _debug_options.DUMP_EXCEPTIONS:
                debug.dump_error(error)
            # Must queue, since there can be captured dependencies to resolve
            self._queue_throw_error(error)

    def _queue_exit(self, result):
        # Result must be unwrapped first,
        # and since there can be dependencies, the error result (exception)
        # is still possible at this point.
        #
        # So we can't instantly close the generator here, since if there is
        # an error during dependency computation, we must re-throw it
        # from the generator.
        if self._value is not _futures_none:  # is_computed(), but faster
            raise futures.FutureIsAlreadyComputed(self)
        if self._generator is not None:
            self._generator.close()
            self._generator = None
        if self._dependencies:  # is_blocked(), but faster
            if _debug_options.DUMP_QUEUED_RESULTS:
                debug.write('@async: queuing exit: %s <- %s' % (debug.repr(result), debug.str(self)))
            self._last_value = result
        else:
            self.set_value(result)

    def _queue_throw_error(self, error):
        if self._value is not _futures_none:  # is_computed(), but faster
            raise futures.FutureIsAlreadyComputed(self)
        if self._generator is not None or self._dependencies:  # can_continue() or is_blocked(), but faster
            if _debug_options.DUMP_QUEUED_RESULTS:
                debug.write('@async: queuing throw error: %s <-x- %s' % (debug.repr(error), debug.str(self)))
            self._last_value = futures.ErrorFuture(error)  # To get it re-thrown on unwrap
            if self.group_cancel:
                self.cancel_dependencies(error)
        else:
            self.set_error(error)

    def _remove_dependency(self, dependency):
        self._remove_dependency_cython(dependency)

    def _remove_dependency_cython(self, dependency):
        if _debug_options.DUMP_DEPENDENCIES:
            debug.write('@async: -dependency: %s got %s' % (debug.str(self), debug.str(dependency)))
        self._dependencies.remove(dependency)
        if self._value is _futures_none:  # not is_computed(), but faster
            try:
                error = dependency.error()
                if not (error is None or isinstance(error, AsyncTaskCancelledError)):
                    self._queue_throw_error(error)
            finally:
                if not self._dependencies:  # not is_blocked(), but faster
                    self.scheduler._schedule_without_checks(self)

    def make_dependency(self, task, scheduler):
        """Mark self as a dependency on task.

        i.e. self needs to be computed before task can be continued further.

        """
        self.depth = task.depth + 1
        if self.max_depth == 0:
            self.max_depth = task.max_depth
        if self.max_depth != 0:
            if self.depth > self.max_depth:
                debug.dump(scheduler)
                assert False, \
                    "Task stack depth exceeded specified maximum (%i)" % self.max_depth
        self.caller = task
        task._dependencies.add(self)
        self.on_computed.subscribe(task._remove_dependency)
        scheduler.schedule(self)

    def _before_continue(self):
        self._resume_contexts()

    def _after_continue(self):
        self._pause_contexts()

    def traceback(self):
        try:
            self_str = self._traceback_line()
        except Exception:
            # If _traceback_line failed for whatever reason (e.g. there is no correct frame_info),
            # fall back to __str__ so that we can still provide useful information for debugging
            self_str = core_helpers.safe_str(self)
        if self.caller is None:
            return [self_str]
        result = self.caller.traceback()
        result.append(self_str)
        return result

    def _traceback_line(self):
        frame_info = self._frame_info
        if frame_info is None and self._generator is not None:
            frame_info = debug.get_frame_info(self._generator)
        if frame_info is not None:
            template = '''File "%(file)s", line %(lineno)s, in %(funcname)s
    %(codeline)s'''
            return template % {
                'file': frame_info.filename,
                'lineno': frame_info.lineno,
                'funcname': frame_info.function,
                'codeline': '\n'.join(frame_info.code_context).strip()
            }
        else:
            return str(self)

    def __str__(self):
        fn_str = core_inspection.get_function_call_str(self.fn, self.args, self.kwargs)
        name = '@async %s' % fn_str

        # we subtract one because by the time the stacktrace is printed
        # the iteration_index has already been incremented
        if self.iteration_index - 1 == 0:
            step = 'before 1st yield'
        else:
            step = 'passed yield #%i' % (self.iteration_index - 1)

        if self.is_computed():
            status = 'computed, '
            if self.error() is None:
                status += '= ' + repr(self.value())
            else:
                status += 'error = ' + repr(self.error())
        else:
            if self.is_blocked():
                status = 'blocked x%i' % len(self._dependencies)
            elif self.can_continue():
                if self.is_scheduled:
                    status = 'scheduled'
                elif self.scheduler:
                    status = 'waiting'
                else:
                    status = 'new'
            else:
                status = 'almost finished (generator is closed)'
                if self.is_scheduled:
                    status += ', scheduled'
        return '%s (%s, %s)' % (name, status, step)

    def dump(self, indent=0):
        if indent > MAX_DUMP_INDENT:
            debug.write('...', indent + 1)
            return
        debug.write(debug.str(self), indent)
        if self._dependencies:
            debug.write('Dependencies:', indent + 1)
            for dependency in self._dependencies:
                dependency.dump(indent + 2)  # Recursive
                # debug.write(debug.str(dependency), indent + 2)
        else:
            debug.write('No dependencies.', indent + 1)

    # Contexts support

    def _enter_context(self, context):
        if _debug_options.DUMP_CONTEXTS:
            debug.write('@async: +context: %s' % debug.str(context))
        self._contexts.append(context)

    def _leave_context(self, context):
        if _debug_options.DUMP_CONTEXTS:
            debug.write('@async: -context: %s' % debug.str(context))
        self._contexts.remove(context)

    def _pause_contexts(self):
        contexts = self._contexts
        i = len(contexts) - 1
        # execute each __pause__() in a try/except and if 1 or more of them
        # raise an exception, then save the last exception raised so that it
        # can be re-raised later. We re-raise the last exception to make the
        # behavior consistent with __exit__.
        error = None
        while i >= 0:
            try:
                contexts[i].__pause__()
            except BaseException as e:
                error = e
                core_errors.prepare_for_reraise(error)
            i -= 1
        if error is not None:
            self._accept_error(error)

    def _resume_contexts(self):
        i = 0
        contexts = self._contexts
        l = len(contexts)
        # same try/except deal as with _pause_contexts, but in this case
        # we re-raise the first exception raised.
        error = None
        while i < l:
            try:
                contexts[i].__resume__()
            except BaseException as e:
                if error is None:
                    error = e
                    core_errors.prepare_for_reraise(error)
            i += 1
        if error is not None:
            self._accept_error(error)


def unwrap(value):
    """
    'Unwraps' the provided arguments by converting:

    * ``FutureBase`` objects to their values;
      consequently, ``AsyncTask`` objects are also
      converted to their values
    * Tuples containing ``FutureBase`` or ``AsyncTask``
      objects are converted to tuples containing their
      values
    * Tuples are processed recursively.

    """
    if value is None:  # Special case
        return None
    elif isinstance(value, futures.FutureBase):
        future = value
        return future.value()
    elif type(value) is tuple:
        tpl = value
        # Simple perf. optimization
        length = len(tpl)
        if length <= 1:
            if length == 0:
                return ()
            return (unwrap(tpl[0]),)
        elif length == 2:
            return (unwrap(tpl[0]), unwrap(tpl[1]))
        else:
            result = []
            for item in tpl:
                result.append(unwrap(item))
            return tuple(result)
    elif type(value) is list:
        lst = value
        return [unwrap(item) for item in lst]
    elif type(value) is dict:
        dct = value
        return {key: unwrap(value) for key, value in six.iteritems(dct)}
    else:
        raise TypeError(
            "Cannot unwrap an object of type '%s': only futures and None are allowed." %
            type(value)
        )


# Private part

_empty_tuple = tuple()
_empty_dictionary = dict()
globals()['_empty_tuple'] = _empty_tuple
globals()['_empty_dictionary'] = _empty_dictionary


def _cancel_futures(value, error):
    """Used by ``AsyncTask._continue`` to cancel evaluation of tasks
    and futures due to failure of one of them.

    """
    if value is None:
        return
    if isinstance(value, AsyncTask):
        if not value.is_computed():
            value._queue_throw_error(error)
    elif isinstance(value, futures.FutureBase):
        if not value.is_computed():
            value.set_error(error)
