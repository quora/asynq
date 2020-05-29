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
import threading

from qcore import utime
import qcore.events as core_events


from . import debug
from . import futures
from . import batching
from . import _debug
from .async_task import AsyncTask

assert str(__name__).endswith("asynq.scheduler") or str(__name__).endswith(
    "asynq.lib64.scheduler"
), "Are you importing asynq from the wrong directory?"

_debug_options = _debug.options
_futures_none = futures._none


class AsyncTaskError(Exception):
    pass


class TaskScheduler(object):
    """Schedules and runs AsyncTask objects flushing batches when needed."""

    def __init__(self):
        self._last_dump_time = time.time()
        self.on_before_batch_flush = core_events.EventHook()
        self.on_after_batch_flush = core_events.EventHook()
        thread = threading.current_thread()
        thread_name = thread.name if thread.name else str(thread.ident)
        if "_state" in globals():
            _state.last_id += 1
            _id = _state.last_id
        else:
            _id = 0
        self.name = "%s / %d" % (thread_name, _id)
        self.reset()

    def reset(self):
        self._batches = set()
        self._tasks = []
        self.active_task = None

    def wait_for(self, task):
        """
        Executes a task and ensures it's complete when this method returns.

        :param tasks: task to wait for
        :return: ``None``
        """
        while not task.is_computed():
            self._execute(task)
            if task.is_computed():
                break
            self._continue_with_batch()

    def _execute(self, root_task):
        """Implements task execution loop.

        The postcondition of this method is that all tasks in the dependency tree of root_task
        that aren't blocked on batch items waiting to be flushed should be executed until they are
        (or until they're computed). This is done by running a depth-first search on the dependency
        tree.

        :param root_task: root of the dependency tree
        :return: ``None``
        """
        init_num_tasks = len(self._tasks)
        self._tasks.append(root_task)

        # Run the execution loop until the root_task is complete (it's either blocked on batch
        # items waiting to be flushed, or computed).
        while len(self._tasks) > init_num_tasks:
            if len(self._tasks) > _debug_options.MAX_TASK_STACK_SIZE:
                self.reset()
                debug.dump(self)
                raise RuntimeError(
                    "Number of scheduled tasks exceeded maximum threshold."
                )

            # _tasks is a stack, so take the last one.
            task = self._tasks[-1]
            if _debug_options.DUMP_SCHEDULER_STATE:
                self.try_time_based_dump()

            if task.is_computed():
                self._tasks.pop()
            elif isinstance(task, AsyncTask):
                self._handle_async_task(task)
            elif isinstance(task, batching.BatchItemBase):
                # This can happen multiple times per batch item (if we run _execute and this batch
                # item doesn't get flushed), but that's ok because self._batches is a set.
                self._schedule_batch(task.batch)
                self._tasks.pop()
            else:
                task._compute()
                self._tasks.pop()

    def _schedule_batch(self, batch):
        if batch.is_flushed():
            if _debug_options.DUMP_SCHEDULE_BATCH:
                debug.write(
                    "@async: can't schedule flushed batch %s" % debug.str(batch)
                )
            return False
        if _debug_options.DUMP_SCHEDULE_BATCH and batch not in self._batches:
            debug.write("@async: scheduling batch %s" % debug.str(batch))
        self._batches.add(batch)
        return True

    def _flush_batch(self, batch):
        self.on_before_batch_flush(batch)
        try:
            if _debug_options.COLLECT_PERF_STATS:
                start = utime()
                batch.flush()
                batch.dump_perf_stats(utime() - start)
            else:
                batch.flush()
        finally:
            self.on_after_batch_flush(batch)
        return 0

    def _handle_async_task(self, task):
        # is_blocked indicates that one of the tasks dependencies isn't computed yet,
        # so we can't run _continue until they are.
        if task.is_blocked():
            # _dependencies_scheduled indicates if we've already added the task's
            # dependencies to the task stack. If the task is blocked and we've already
            # scheduled and run its dependencies, it's blocked on batch items waiting
            # to be flushed so we're done with this task.
            if task._dependencies_scheduled:
                # Set _dependencies_scheduled to false so on future runs of _execute,
                # we add the dependencies to the task stack (since some of the batch items
                # in the subtree might have been flushed)
                if _debug_options.DUMP_CONTINUE_TASK:
                    debug.write("@async: skipping %s" % debug.str(task))
                task._dependencies_scheduled = False
                task._pause_contexts()
                self._tasks.pop()
            # If the task is blocked and we haven't scheduled its dependencies, we
            # should do so now.
            else:
                task._dependencies_scheduled = True
                task._resume_contexts()
                for dependency in task._dependencies:
                    if not dependency.is_computed():
                        if _debug_options.DUMP_SCHEDULE_TASK:
                            debug.write(
                                "@async: scheduling task %s" % debug.str(dependency)
                            )
                        if _debug_options.DUMP_DEPENDENCIES:
                            debug.write(
                                "@async: +dependency: %s needs %s"
                                % (debug.str(task), debug.str(dependency))
                            )
                        self._tasks.append(dependency)
        else:
            self._continue_with_task(task)

    def _continue_with_task(self, task):
        task._resume_contexts()
        old_task = self.active_task
        self.active_task = task

        if _debug_options.DUMP_CONTINUE_TASK:
            debug.write("@async: -> continuing %s" % debug.str(task))
        if _debug_options.COLLECT_PERF_STATS:
            start = utime()
            task._continue()
            task._total_time += utime() - start
            if task.is_computed():
                task.dump_perf_stats()
        else:
            task._continue()
        if _debug_options.DUMP_CONTINUE_TASK:
            debug.write("@async: <- continued %s" % debug.str(task))

        self.active_task = old_task
        # We get a new set of dependencies when we run _continue, so these haven't
        # been scheduled.
        task._dependencies_scheduled = False

    def _continue_with_batch(self):
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
                debug.write("@async: no batch to flush")
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

    def __str__(self):
        return "%s %s (%d tasks, %d batches; active task: %s)" % (
            type(self),
            repr(self.name),
            len(self._tasks),
            len(self._batches),
            str(self.active_task),
        )

    def __repr__(self):
        return self.__str__()

    def dump(self, indent=0):
        debug.write(debug.str(self), indent)
        if self._tasks:
            debug.write("Task queue:", indent + 1)
            for task in self._tasks:
                task.dump(indent + 2)
        else:
            debug.write("No tasks in task queue.", indent + 1)
        if self._batches:
            debug.write("Batches:", indent + 1)
            for batch in self._batches:
                batch.dump(indent + 2)

    def try_time_based_dump(self, last_task=None):
        current_time = time.time()
        if (
            current_time - self._last_dump_time
        ) < _debug_options.SCHEDULER_STATE_DUMP_INTERVAL:
            return
        self._last_dump_time = current_time
        debug.write(
            "\n--- Scheduler state dump: --------------------------------------------"
        )
        try:
            self.dump()
            if last_task is not None:
                debug.write("Last task: %s" % debug.str(last_task), 1)
        finally:
            debug.write(
                "----------------------------------------------------------------------\n"
            )
            stdout.flush()
            stderr.flush()


class LocalTaskSchedulerState(threading.local):
    def __init__(self):
        self.last_id = 0
        self.reset()

    def reset(self):
        self.current = TaskScheduler()


_state = LocalTaskSchedulerState()
globals()["_state"] = _state


def get_scheduler():
    global _state
    return _state.current


def reset():
    _state.reset()


def get_active_task():
    global _state
    s = _state.current
    return None if s is None else s.active_task
