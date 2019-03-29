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
cimport qcore.helpers as core_helpers
cimport qcore.inspection as core_inspection
cimport futures
cimport scheduler
cimport _debug
cimport profiler
cimport batching


cdef _debug.DebugOptions _debug_options
cdef object _futures_none
cdef object _none_future


cdef class AsyncTask(futures.FutureBase):
    cdef public long iteration_index
    cdef public object fn
    cdef public object args
    cdef public object kwargs
    cdef public str _name
    cdef public AsyncTask creator
    cdef public bint running
    cdef public object _generator
    cdef public object _last_value
    cdef public object _frame
    cdef public object perf_stats
    cdef public list _dependencies
    cdef public object _contexts
    cdef public bint _contexts_active
    cdef public bint _dependencies_scheduled
    cdef public int _total_time
    cdef public int _id

    cdef bint is_blocked(self) except -1
    cdef bint can_continue(self) except -1
    cdef dump_perf_stats(self)
    cdef collect_perf_stats(self)

    cpdef _compute(self)
    cpdef _computed(self)
    cpdef to_str(self)

    cdef _continue(self)
    cdef inline object _continue_on_generator(self, value, error)

    @cython.locals(scheduler=scheduler.TaskScheduler)
    cdef inline object _accept_yield_result(self, result)
    cdef inline object _accept_error(self, error)

    cdef _queue_exit(self, result)
    cdef _queue_throw_error(self, error)

    cpdef list traceback(self)

    cpdef dump(self, int indent=?)

    # Contexts support

    cdef _enter_context(self, context)
    cdef _leave_context(self, context)
    @cython.locals(i=int)
    cdef _pause_contexts(self)
    @cython.locals(i=int)
    cdef _resume_contexts(self)


@cython.locals(length=int, future=futures.FutureBase, tpl=tuple, lst=list, dct=dict)
cdef object unwrap(object value)

cpdef extract_futures(object value, list result)

cdef tuple _empty_tuple
cdef dict _empty_dictionary
