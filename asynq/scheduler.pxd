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

import cython
from cpython.ref cimport PyObject

cimport qcore.events as core_events
cimport futures
cimport async_task
cimport batching
cimport _debug


cdef _debug.DebugOptions _debug_options
cdef object _futures_none


cdef class TaskScheduler(object):
    cdef public set _empty_set
    cdef public float _last_dump_time
    cdef public list _new_tasks
    cdef public object _tasks
    cdef public set _batches
    cdef public int await_recursion_depth
    cdef public int max_await_recursion_depth
    cdef public core_events.EventHook on_before_batch_flush
    cdef public core_events.EventHook on_after_batch_flush
    cdef public async_task.AsyncTask active_task
    cdef public str name

    cpdef TaskSchedulerActivator activate(self, bint must_await_on_exit, bint must_flush_batches_on_exit)
    cpdef reset(self)

    cpdef async_task.AsyncTask start(self, async_task.AsyncTask task,
            bint run_immediately=?)
    cpdef async_task.AsyncTask continue_with(self, futures.FutureBase future, async_task.AsyncTask task)
    cpdef int flush_batches(self, bint must_await=?) except -1

    cpdef add_dependencies(self, async_task.AsyncTask task, object yield_result)

    cpdef add_dependency(self, async_task.AsyncTask task, futures.FutureBase dependency)
    cpdef schedule(self, async_task.AsyncTask task)
    cpdef schedule_batch(self, batching.BatchBase batch)

    @cython.locals(l=int, active_task=async_task.AsyncTask)
    cpdef int _await(self, list tasks) except -1
    @cython.locals(dump_scheduler_state=bint, dump_await_recursion=bint,
                   task=async_task.AsyncTask, creator=async_task.AsyncTask,
                   captured_dependencies=list)
    cpdef int _execute(self, bint is_partial_execution, set tasks) except -1
    @cython.locals(new_scheduler=TaskScheduler, new_scheduler_tasks=list,
                   task=async_task.AsyncTask)
    cpdef _try_execute_on_new_scheduler(self, set tasks)

    cdef bint _schedule(self, async_task.AsyncTask task) except -1
    cdef bint _schedule_without_checks(self, async_task.AsyncTask task) except -1
    cdef bint _try_schedule(self, async_task.AsyncTask task, bint enqueue_new_tasks) except -1
    cdef bint _schedule_batch(self, batching.BatchBase batch) except -1
    cdef int _flush_batch(self, batching.BatchBase batch) except -1

    @cython.locals(l=int, i=int)
    cdef inline object _enqueue_new_tasks(self)
    @cython.locals(dump_continue_task=bint, task=async_task.AsyncTask)
    cdef async_task.AsyncTask _continue_with_task(self)
    cdef batching.BatchBase _continue_with_batch(self, bint assert_no_batch=?)

    cpdef batching.BatchBase _select_batch_to_flush(self)

    @cython.locals(clone=TaskScheduler)
    cpdef object _clone(self)
    cpdef _accept_changes(self, TaskScheduler clone)

    cpdef dump(self, int indent=?)
    @cython.locals(current_time=float)
    cpdef try_time_based_dump(self, object last_task=?)

cdef class TaskSchedulerActivator(object):
    cdef TaskScheduler _scheduler
    cdef TaskScheduler _old_scheduler
    cdef bint _must_await_on_exit
    cdef bint _must_flush_batches_on_exit

cpdef object extract_futures(object value, list result)

cdef object _state

cpdef TaskScheduler get_scheduler()
cpdef set_scheduler(scheduler)

cpdef async_task.AsyncTask get_active_task()
cpdef bint _no_blocked_tasks(tasks) except -1

cpdef futures.FutureBase _create_blocking_dependency()
