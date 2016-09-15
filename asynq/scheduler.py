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

from sys import stderr, stdout
import time
import collections
import threading

import qcore.events as core_events


from . import debug
from . import futures
from . import batching
from . import _debug


assert str(__name__).endswith('asynq.scheduler') \
    or str(__name__).endswith('asynq.lib64.scheduler'), \
    "Are you importing asynq from the wrong directory?"


MAX_AWAIT_RECURSION_DEPTH = 10

_debug_options = _debug.options
_futures_none = futures._none


class AsyncTaskError(Exception):
    pass


class TaskScheduler(object):
    """Schedules and runs AsyncTask objects flushing batches when needed."""

    def __init__(self):
        self._last_dump_time = time.time()
        self._empty_set = set()  # Just a perf. optimization
        self.await_recursion_depth = 0
        self.max_await_recursion_depth = MAX_AWAIT_RECURSION_DEPTH
        self.on_before_batch_flush = core_events.EventHook()
        self.on_after_batch_flush = core_events.EventHook()
        self.reset()
        thread = threading.current_thread()
        thread_name = thread.name if thread.name else str(thread.ident)
        if '_state' in globals():
            _state.last_id += 1
            _id = _state.last_id
        else:
            _id = 0
        self.name = '%s / %d' % (thread_name, _id)

    def activate(self, must_await_on_exit, must_flush_batches_on_exit):
        return TaskSchedulerActivator(self, must_await_on_exit, must_flush_batches_on_exit)

    def reset(self):
        """Resets the scheduler to its initial state."""
        # Used to reverse the sequence of newly scheduled tasks
        # to execute them in correct order
        self._new_tasks = []
        self._tasks = collections.deque()
        self._batches = set()
        self.active_task = None

    def start(self, task, run_immediately=False):
        """Starts the specified task by simply scheduling it.
        Returns the task passed to this method.

        """
        if task.is_computed():
            return task
        if task.scheduler is not None:
            raise AsyncTaskError('Task is already scheduled.')
        if task.is_blocked():
            raise AsyncTaskError("Task is blocked, so it shouldn't be scheduled.")

        if run_immediately:
            self._enqueue_new_tasks()
            self._schedule(task)
            self._continue_with_task()
        else:
            self._schedule(task)
        return task

    def continue_with(self, future, task):
        """Starts (or continues) the task after specified future gets computed.
        Returns the task passed to this method.

        """
        if task.is_computed():
            raise AsyncTaskError('Task is already computed.')
        if future.is_computed():
            if task.scheduler is None:
                return self.start(task)
            else:
                # Nothing to do in this case
                return task
        if task.scheduler is None:
            task.scheduler = self
        elif task.scheduler is not self:
            raise AsyncTaskError('Task is already scheduled on another scheduler.')
        self.add_dependency(task, future)
        self._try_schedule(task, True)
        return task

    def flush_batches(self, must_await=False):
        """Flushes all the batches that are currently 'known'
        to scheduler. Normally you shouldn't call this method.

        """
        if must_await:
            self.await()

        error = None
        while self._batches:
            batches = self._batches
            self._batches = set()
            for batch in batches:
                try:
                    if batch.items and not batch.is_flushed():
                        self._flush_batch(batch)
                except BaseException as e:
                    if error is None:
                        error = e
        if error is not None:
            raise error
        return 0

    def await(self, tasks=[]):
        """
        Executes a list of tasks ensuring all of them are completed when this method returns.

        :param tasks: tasks to await for
        :return: ``None``
        """
        self._await(tasks)

    def add_dependencies(self, task, yield_result):
        """Mark all futures in yield_result as dependencies for task.

        i.e. everything in yield_result needs to be computed before task can be continued.

        """
        if yield_result is None:  # Frequent case
            return
        for future in extract_futures(yield_result, []):
            self.add_dependency(task, future)

    def add_dependency(self, task, dependency):
        if dependency.is_computed():
            return
        if dependency in task._dependencies:
            if _debug_options.DUMP_DEPENDENCIES:
                debug.write('@async: +dependency: %s already depends on %s' %
                    (debug.str(task), debug.str(dependency)))
            return
        if _debug_options.DUMP_DEPENDENCIES:
            debug.write('@async: +dependency: %s needs %s' %
                (debug.str(task), debug.str(dependency)))
        dependency.make_dependency(task, self)

    def _await(self, tasks):
        active_task = self.active_task
        if active_task is not None:
            # Since task we await for can be (and most likely is)
            # a child of the current one, it already copied the
            # list of its contexts; so when we start it, these
            # contexts are going to be resumed, but we don't want
            # to resume already active contexts. That's why we
            # need to pause them.
            active_task._pause_contexts()
            # We must temporarily block active task in case
            # it's somehow (c) added to scheduler's queue
            blocking_dependency = _create_blocking_dependency()
            active_task._dependencies.add(blocking_dependency)
        try:
            # Must never throw an exception
            if len(tasks) == 0:
                # No arguments = run() must empty task queue
                return self._execute(False, self._empty_set)
            else:
                return self._execute(True, set(tasks))
        finally:
            if active_task is not None:
                active_task._dependencies.remove(blocking_dependency)
                active_task._resume_contexts()
        return 0

    def _execute(self, is_partial_execution, tasks):
        """Implements task execution loop."""
        # Must never throw an exception
        self.await_recursion_depth += 1
        current_active_task = self.active_task
        self.active_task = None
        try:
            if self.await_recursion_depth > self.max_await_recursion_depth:
                # We don't want to execute every next task on
                # stack in case all of these tasks invoke
                # await at some point
                dump_await_recursion = _debug_options.DUMP_AWAIT_RECURSION
                if dump_await_recursion:
                    debug.write("@async: await recursion: depth is %d, but the limit is %d" %
                                (self.await_recursion_depth, self.max_await_recursion_depth))
                if not is_partial_execution:
                    if dump_await_recursion:
                        debug.write("@async: await recursion: ignoring await()")
                    return 0
                self._try_execute_on_new_scheduler(tasks)
                # The rest part of tasks will be processed by this scheduler
            dump_scheduler_state = _debug_options.DUMP_SCHEDULER_STATE
            if is_partial_execution and len(tasks) == 0:
                return 0
            for task in tasks:
                creator = task.creator
                if creator is not None:
                    if task in creator._dependencies:
                        creator._remove_dependency(task)
                self._schedule(task)
            while True:  # Batch loop
                while True:  # Task loop
                    task = self._continue_with_task()
                    if dump_scheduler_state:
                        self.try_time_based_dump(task)
                    if task is None:
                        if _no_blocked_tasks(tasks):
                            return 0  # All specified tasks are computed
                        break  # No more tasks - let's try to flush the batch
                    if is_partial_execution and task.is_computed() and task in tasks:
                        tasks.remove(task)
                        if len(tasks) == 0:
                            return 0  # All specified tasks are computed
                try:
                    batch = self._continue_with_batch(is_partial_execution)
                    if batch is None:
                        return 0  # Nothing more to execute
                except AssertionError:
                    if _debug_options.DUMP_PRE_ERROR_STATE:
                        debug.write('Scheduler is awaiting for:')
                        for task in tasks:
                            task.dump(2)
                        debug.write('----------------------------------------------------------------------\n')
                        raise
        finally:
            self.active_task = current_active_task
            self.await_recursion_depth -= 1
        return 0

    def _try_execute_on_new_scheduler(self, tasks):
        new_scheduler_tasks = []
        for task in tasks:
            if task.scheduler is None:
                new_scheduler_tasks.append(task)
        for task in new_scheduler_tasks:
            tasks.remove(task)

        if _debug_options.DUMP_AWAIT_RECURSION:
            debug.write("@async: await recursion: going to run %d tasks on a new scheduler" % len(new_scheduler_tasks))

        if len(new_scheduler_tasks) == 0:
            pass
        else:
            new_scheduler = self._clone()
            try:
                with new_scheduler.activate(True, False):
                    new_scheduler.await(new_scheduler_tasks)
            finally:
                self._accept_changes(new_scheduler)

    def schedule(self, task):
        self._schedule(task)

    def _schedule(self, task):
        if task.is_blocked():
            if _debug_options.DUMP_SCHEDULE_TASK:
                debug.write("@async: can't schedule blocked task %s" % debug.str(task))
            assert task.scheduler is self, \
                'Task %s is already scheduled on another scheduler.' % debug.str(task)
            return False
        if task.is_computed():
            if _debug_options.DUMP_SCHEDULE_TASK:
                debug.write("@async: can't schedule computed task %s" % debug.str(task))
            return False
        return self._schedule_without_checks(task)

    def _schedule_without_checks(self, task):
        if _debug_options.DUMP_SCHEDULE_TASK:
            debug.write('@async: scheduling task %s' % debug.str(task))

        if task.scheduler is None:
            task.scheduler = self
        else:
            assert task.scheduler is self, 'Task is already scheduled on another scheduler.'

        if not task.is_scheduled:
            task.is_scheduled = True
            self._new_tasks.append(task)
        return True

    def _try_schedule(self, task, enqueue_new_tasks):
        if task.is_computed() or task.is_blocked():
            return False
        if enqueue_new_tasks:
            self._enqueue_new_tasks()
        return self._schedule_without_checks(task)

    def schedule_batch(self, batch):
        self._schedule_batch(batch)

    def _schedule_batch(self, batch):
        if batch.is_flushed():
            if _debug_options.DUMP_SCHEDULE_BATCH:
                debug.write("@async: can't schedule flushed batch %s" % debug.str(batch))
            return False
        if _debug_options.DUMP_SCHEDULE_BATCH and batch not in self._batches:
            debug.write('@async: scheduling batch %s' % debug.str(batch))
        self._batches.add(batch)
        return True

    def _flush_batch(self, batch):
        self.on_before_batch_flush(batch)
        try:
            batch.flush()
        finally:
            self.on_after_batch_flush(batch)
        return 0

    def _enqueue_new_tasks(self):
        l = len(self._new_tasks)
        if l == 0:
            pass
        else:
            i = 0
            while i < l:
                self._tasks.append(self._new_tasks[i])
                i += 1
            del self._new_tasks[:]

    def _continue_with_task(self):
        """Continues the next task.
        Returns the task that made some progress or None.

        """
        # Queuing new tasks
        self._enqueue_new_tasks()
        dump_continue_task = _debug_options.DUMP_CONTINUE_TASK
        while len(self._tasks) != 0:
            task = self._tasks.popleft()
            task.is_scheduled = False
            if task._dependencies or task._value is not _futures_none:  # is_blocked() or is_computed(), but faster
                if dump_continue_task:
                    debug.write('@async: skipping %s' % debug.str(task))
                continue

            # Executing the task
            old_active_task = self.active_task
            self.active_task = task
            try:
                if dump_continue_task:
                    debug.write('@async: -> continuing %s' % debug.str(task))
                task._continue()
            finally:
                # task._continue should never throw an error, but
                # it's better to implement active task switch in safe way
                self.active_task = old_active_task
                # No need to reschedule: task._continue() ensures this
                # anyway
                if dump_continue_task:
                    debug.write('@async: <- continued %s' % debug.str(task))
            return task
        if dump_continue_task:
            debug.write('@async: no task to continue')
        return None

    def _continue_with_batch(self, assert_no_batch=True):
        """
        Flushes one of batches (the longest one by default).

        :param assert_no_batch: indicates whether exception must be
                                raised if there is no batch to flush
        :return: the batch that was flushed, if there was a flush;
                 otherwise, ``None``.

        """
        batch = self._select_batch_to_flush()
        if batch is None:
            if _debug_options.DUMP_FLUSH_BATCH:
                debug.write('@async: no batch to flush')
            if assert_no_batch:
                debug.dump(self)
                assert False, \
                    "Can't proceed further: no task to continue or batch to flush, active tasks: " \
                    "%s" % self._tasks
            else:
                return None
        self._batches.remove(batch)
        self._flush_batch(batch)
        return batch

    def _select_batch_to_flush(self):
        """Returns the batch having highest priority,
        or ``None``, if there are no batches.

        This method uses ``BatchBase.get_priority()`` to
        determine the priority.

        Side effect: this method removed flushed batches.

        :return: selected batch or None.

        """
        best_batch = None
        best_priority = None
        batches_to_remove = None
        for batch in self._batches:
            if not batch.items or batch.is_flushed():
                if batches_to_remove is None:
                    batches_to_remove = [batch]
                else:
                    batches_to_remove.append(batch)
                continue
            priority = batch.get_priority()
            if best_batch is None or best_priority < priority:
                best_batch = batch
                best_priority = priority
        if batches_to_remove:
            for batch in batches_to_remove:
                self._batches.remove(batch)
        return best_batch

    def _clone(self):
        """Protected method cloning the scheduler.

        This method is used to create a new scheduler temporarily
        replacing the current one to handle await recursion

        """
        clone = type(self)()
        clone._last_dump_time = self._last_dump_time
        clone.max_await_recursion_depth = self.max_await_recursion_depth
        clone.on_before_batch_flush = self.on_before_batch_flush
        clone.on_after_batch_flush = self.on_after_batch_flush
        return clone

    def _accept_changes(self, clone):
        """Protected method accepting the changes made to this scheduler's clone."""

        self._last_dump_time = clone._last_dump_time
        self.max_await_recursion_depth = clone.max_await_recursion_depth
        self.on_before_batch_flush = clone.on_before_batch_flush
        self.on_after_batch_flush = clone.on_after_batch_flush
        for batch in clone._batches:
            self._batches.add(batch)

    def __str__(self):
        return '%s %s (%d + %d tasks, %d batches; active task: %s)' % \
            (type(self), repr(self.name),
             len(self._tasks), len(self._new_tasks), len(self._batches),
             str(self.active_task))

    def __repr__(self):
        return self.__str__()

    def dump(self, indent=0):
        debug.write(debug.str(self), indent)
        if self._tasks:
            debug.write('Task queue:', indent + 1)
            for task in self._tasks:
                task.dump(indent + 2)
        else:
            debug.write('No tasks in task queue.', indent + 1)
        if self._new_tasks:
            debug.write('New tasks:', indent + 1)
            for task in self._new_tasks:
                task.dump(indent + 2)
        if self._batches:
            debug.write('Batches:', indent + 1)
            for batch in self._batches:
                batch.dump(indent + 2)

    def try_time_based_dump(self, last_task=None):
        current_time = time.time()
        if (current_time - self._last_dump_time) < _debug_options.SCHEDULER_STATE_DUMP_INTERVAL:
            return
        self._last_dump_time = current_time
        debug.write('\n--- Scheduler state dump: --------------------------------------------')
        try:
            self.dump()
            if last_task is not None:
                debug.write('Last task: %s' % debug.str(last_task), 1)
        finally:
            debug.write('----------------------------------------------------------------------\n')
            stdout.flush()
            stderr.flush()


class TaskSchedulerActivator(object):
    def __init__(self, scheduler, must_await_on_exit, must_flush_batches_on_exit):
        self._scheduler = scheduler
        self._old_scheduler = None
        self._must_await_on_exit = must_await_on_exit
        self._must_flush_batches_on_exit = must_flush_batches_on_exit

    def __enter__(self):
        if _debug_options.DUMP_SCHEDULER_CHANGE:
            debug.write("@async: -> scheduler activation")
        self._old_scheduler = get_scheduler()
        set_scheduler(self._scheduler)

    def __exit__(self, type, value, traceback):
        try:
            try:
                if self._must_await_on_exit:
                    self._scheduler.await()
            finally:
                if self._must_flush_batches_on_exit:
                    self._scheduler.flush_batches()
        finally:
            set_scheduler(self._old_scheduler)
            if _debug_options.DUMP_SCHEDULER_CHANGE:
                debug.write("@async: <- scheduler activation")


class LocalTaskSchedulerState(threading.local):
    def __init__(self):
        self.last_id = 0
        self.reset()

    def reset(self):
        self.current = TaskScheduler()

_state = LocalTaskSchedulerState()
globals()['_state'] = _state


def get_scheduler():
    global _state
    return _state.current


def reset():
    _state.reset()


def set_scheduler(scheduler):
    global _state
    _state.current = scheduler
    if _debug_options.DUMP_SCHEDULER_CHANGE:
        debug.write("@async: scheduler = %s" % debug.str(scheduler))


def get_active_task():
    global _state
    s = _state.current
    return None if s is None else s.active_task


def extract_futures(value, result):
    """Enumerates all the futures inside a particular value."""
    if value is None:
        pass
    elif isinstance(value, futures.FutureBase):
        result.append(value)
    elif type(value) is tuple or type(value) is list:
        for item in value:
            extract_futures(item, result)
    return result


def _no_blocked_tasks(tasks):
    computed = [task for task in tasks if task.is_computed()]
    for task in computed:
        if task in tasks:
            tasks.remove(task)
    return len(tasks) == 0


def _create_blocking_dependency():
    return futures.ConstFuture("async_blocking_dependency")
