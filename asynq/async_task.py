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
import sys

import qcore.helpers as core_helpers
import qcore.inspection as core_inspection
import qcore.errors as core_errors

from collections import OrderedDict

from . import debug
from . import futures
from . import profiler
from . import _debug
from . import batching

import asynq

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

    def __init__(self, generator, fn, args, kwargs):
        super().__init__()
        if _debug_options.ENABLE_COMPLEX_ASSERTIONS:
            assert core_inspection.is_cython_or_generator(generator), (
                "generator is expected, but %s is provided" % generator
            )
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.iteration_index = 0
        self._generator = generator
        self._frame = None
        self._last_value = None
        self._dependencies = []
        self._contexts = OrderedDict()
        self._contexts_active = False
        self._dependencies_scheduled = False
        self._total_time = 0
        self._name = None
        self.perf_stats = {}
        self.creator = asynq.scheduler.get_active_task()
        self.running = False
        if _debug_options.DUMP_NEW_TASKS:
            debug.write(
                "@async: new task: %s, created by %s"
                % (debug.str(self), debug.str(self.creator))
            )
        if _debug_options.COLLECT_PERF_STATS:
            self._id = profiler.incr_counter()

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
        for dependency in self._dependencies:
            if not dependency.is_computed():
                return True
        return False

    def _compute(self):
        if _debug_options.DUMP_SYNC_CALLS:
            if self.creator is not None:
                debug.write(
                    "@async: %s called synchronously from %s"
                    % (debug.str(self), debug.str(self.creator))
                )
        # Forwards the call to task scheduler
        asynq.scheduler.get_scheduler().wait_for(self)
        # No need to assign a value/error here, since
        # _continue method (called by TaskScheduler) does this.

    def dump_perf_stats(self):
        self.perf_stats["time_taken"] = self._total_time
        profiler.append(self.perf_stats)

    def to_str(self):
        if self._name is None:
            try:
                self._name = "%06d.%s(%r %r)" % (
                    self._id,
                    core_inspection.get_full_name(self.fn),
                    self.args,
                    self.kwargs,
                )
            except RuntimeError:
                self._name = "%06d.%s" % (
                    self._id,
                    core_inspection.get_full_name(self.fn),
                )
        return self._name

    def collect_perf_stats(self):
        self.perf_stats = {
            "name": self.to_str(),
            "num_deps": len(self._dependencies),
            "dependencies": [
                (t.to_str(), t._total_time)
                for t in self._dependencies
                if isinstance(t, (AsyncTask, batching.BatchItemBase))
            ],
        }

    def _computed(self):
        try:
            if self._generator is not None:
                self._generator.close()
                self._generator = None
            if _debug_options.COLLECT_PERF_STATS is True:
                self.collect_perf_stats()
        finally:
            # super() doesn't work in Cython-ed version here
            self._dependencies = []
            self._last_value = None
            futures.FutureBase._computed(self)
            error = self.error()

    def _continue(self):
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
                try:
                    # We're on a Python version that supports adding a value to StopIteration
                    return_value = error.value
                except AttributeError:
                    # This means there was no asynq.result() call, so the value of
                    # this task should be None
                    return_value = None
                self._queue_exit(return_value)
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

            if self.is_computed():
                return
            if len(self._dependencies) > 0:
                return

    def _continue_on_generator(self, value, error):
        try:
            if error is None:
                if self._generator is None:
                    raise StopIteration()
            elif self._generator is None:
                raise error

            self.iteration_index += 1
            self._last_value = None
            if not _debug_options.KEEP_DEPENDENCIES:
                self._dependencies = []  # get rid of dependencies to avoid OOM
            if error is None:
                self.running = True
                return self._generator.send(value)
            else:
                self._frame = debug.get_frame(self._generator)
                if hasattr(error, "_task"):
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
            if self._frame is None:
                tb = sys.exc_info()[2]

                while tb.tb_next is not None:
                    tb = tb.tb_next
                self._frame = tb.tb_frame
            self._generator = None
            raise
        finally:
            self.running = False

    def _accept_yield_result(self, result):
        if _debug_options.DUMP_YIELD_RESULTS:
            debug.write(
                "@async: yield: %s -> %s" % (debug.str(self), debug.repr(result))
            )
        self._last_value = result
        extract_futures(result, self._dependencies)

    def _accept_error(self, error):
        if self._value is not _futures_none:  # is_computed(), but faster
            # We can't change the value in this case, so we can just return here.
            pass
        else:
            # when there is no _task it means that this is the bottommost level of the async
            # task. We must attach the traceback as soon as possible
            if not hasattr(error, "_task"):
                error._task = self
                core_errors.prepare_for_reraise(error)
            else:
                # when we already have the _task on the error, it means that
                # some child generator of ours had an error.
                # now, we are storing the _traceback on the error. we use this so we can
                # raise it with that exact traceback later.
                # now, we when do raise it, the upper level gets a new traceback
                # with the current level's traceback connected via a linked list pointer.
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
        # Result must be unwrapped first.
        # All of the dependencies must have been computed at this point.
        if self._value is not _futures_none:  # is_computed(), but faster
            raise futures.FutureIsAlreadyComputed(self)
        if self._generator is not None:
            self._generator.close()
            self._generator = None
        if _debug_options.DUMP_QUEUED_RESULTS:
            debug.write(
                "@async: queuing exit: %s <- %s" % (debug.repr(result), debug.str(self))
            )
        self.set_value(result)

    def _queue_throw_error(self, error):
        if self._value is not _futures_none:  # is_computed(), but faster
            raise futures.FutureIsAlreadyComputed(self)
        if _debug_options.DUMP_QUEUED_RESULTS:
            debug.write(
                "@async: queuing throw error: %s <-x- %s"
                % (debug.repr(error), debug.str(self))
            )
        self.set_error(error)

    def traceback(self):
        try:
            self_str = self._traceback_line()
        except Exception:
            # If _traceback_line failed for whatever reason (e.g. there is no correct frame),
            # fall back to __str__ so that we can still provide useful information for debugging
            self_str = core_helpers.safe_str(self)
        if self.creator is None:
            return [self_str]
        result = self.creator.traceback()
        result.append(self_str)
        return result

    def _traceback_line(self):
        frame = self._frame
        if frame is None and self._generator is not None:
            frame = debug.get_frame(self._generator)

        if frame is not None:
            frame_info = inspect.getframeinfo(frame)
            template = """File "%(file)s", line %(lineno)s, in %(funcname)s
    %(codeline)s"""
            return template % {
                "file": frame_info.filename,
                "lineno": frame_info.lineno,
                "funcname": frame_info.function,
                "codeline": "\n".join(frame_info.code_context).strip(),
            }
        else:
            return str(self)

    def __str__(self):
        fn_str = core_inspection.get_function_call_str(self.fn, self.args, self.kwargs)
        name = "@asynq %s" % fn_str

        # we subtract one because by the time the stacktrace is printed
        # the iteration_index has already been incremented
        if self.iteration_index - 1 == 0:
            step = "before 1st yield"
        else:
            step = "passed yield #%i" % (self.iteration_index - 1)

        if self.is_computed():
            status = "computed, "
            if self.error() is None:
                status += "= " + repr(self.value())
            else:
                status += "error = " + repr(self.error())
        else:
            if self.is_blocked():
                status = "blocked x%i" % len(self._dependencies)
            elif self.can_continue():
                status = "waiting"
            else:
                status = "almost finished (generator is closed)"
        return "%s (%s, %s)" % (name, status, step)

    def dump(self, indent=0):
        if indent > MAX_DUMP_INDENT:
            debug.write("...", indent + 1)
            return
        debug.write(debug.str(self), indent)
        if self._dependencies:
            debug.write("Dependencies:", indent + 1)
            for dependency in self._dependencies:
                dependency.dump(indent + 2)  # Recursive
                # debug.write(debug.str(dependency), indent + 2)
        else:
            debug.write("No dependencies.", indent + 1)

    # Contexts support

    def _enter_context(self, context):
        if _debug_options.DUMP_CONTEXTS:
            debug.write("@async: +context: %s" % debug.str(context))
        self._contexts[id(context)] = context

    def _leave_context(self, context):
        if _debug_options.DUMP_CONTEXTS:
            debug.write("@async: -context: %s" % debug.str(context))
        del self._contexts[id(context)]

    def _pause_contexts(self):
        if not self._contexts_active:
            return
        self._contexts_active = False
        # execute each pause() in a try/except and if 1 or more of them
        # raise an exception, then save the last exception raised so that it
        # can be re-raised later. We re-raise the last exception to make the
        # behavior consistent with __exit__.
        error = None
        for ctx in reversed(list(self._contexts.values())):
            try:
                ctx.pause()
            except BaseException as e:
                error = e
                core_errors.prepare_for_reraise(error)
        if error is not None:
            self._accept_error(error)

    def _resume_contexts(self):
        if self._contexts_active:
            return
        self._contexts_active = True
        # same try/except deal as with _pause_contexts, but in this case
        # we re-raise the first exception raised.
        error = None
        for ctx in self._contexts.values():
            try:
                ctx.resume()
            except BaseException as e:
                if error is None:
                    error = e
                    core_errors.prepare_for_reraise(error)
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
        return {key: unwrap(value) for key, value in dct.items()}
    else:
        raise TypeError(
            "Cannot unwrap an object of type '%s': only futures and None are allowed."
            % type(value)
        )


# Private part

_empty_tuple = tuple()
_empty_dictionary = dict()
globals()["_empty_tuple"] = _empty_tuple
globals()["_empty_dictionary"] = _empty_dictionary


def extract_futures(value, result):
    """Enumerates all the futures inside a particular value."""
    if value is None:
        pass
    elif isinstance(value, futures.FutureBase):
        result.append(value)
    elif type(value) is tuple or type(value) is list:
        # backwards because tasks are added to a stack, so the last one executes first
        i = len(value) - 1
        while i >= 0:
            extract_futures(value[i], result)
            i -= 1
    elif type(value) is dict:
        for item in value.values():
            extract_futures(item, result)
    return result
