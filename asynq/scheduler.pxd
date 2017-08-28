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
    cdef public float _last_dump_time
    cdef public set _batches
    cdef public core_events.EventHook on_before_batch_flush
    cdef public core_events.EventHook on_after_batch_flush
    cdef public async_task.AsyncTask active_task
    cdef public str name
    cdef public list _tasks

    cdef reset(self)

    cpdef int wait_for(self, async_task.AsyncTask task) except -1
    cdef int _execute(self, async_task.AsyncTask root_task) except -1

    cdef _schedule_batch(self, batching.BatchBase batch)
    cdef int _flush_batch(self, batching.BatchBase batch) except -1

    cdef _handle_async_task(self, async_task.AsyncTask task)
    cdef int _continue_with_task(self, async_task.AsyncTask task) except -1
    cdef batching.BatchBase _continue_with_batch(self)
    cdef batching.BatchBase _select_batch_to_flush(self)

    cpdef dump(self, int indent=?)
    @cython.locals(current_time=float)
    cpdef try_time_based_dump(self, object last_task=?)

cdef object _state

cpdef TaskScheduler get_scheduler()

cpdef async_task.AsyncTask get_active_task()
