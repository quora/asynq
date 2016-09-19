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


cdef _debug.DebugOptions _debug_options
cdef object _futures_none
cdef object _none_future


cdef class AsyncTask(futures.FutureBase):
    cdef public bint is_scheduled
    cdef public long iteration_index
    cdef public bint group_cancel
    cdef public object fn
    cdef public object args
    cdef public object kwargs
    cdef public AsyncTask caller
    cdef public int depth
    cdef public int max_depth
    cdef public scheduler.TaskScheduler scheduler
    cdef public AsyncTask creator
    cdef public object _generator
    cdef public object _last_value
    cdef public object _frame_info
    cdef public set _dependencies
    cdef public list _contexts

    cpdef bint is_blocked(self) except -1
    cpdef bint can_continue(self) except -1

    @cython.locals(s=scheduler.TaskScheduler)
    cpdef AsyncTask start(self, bint run_immediately=?)
    @cython.locals(s=scheduler.TaskScheduler)
    cpdef AsyncTask after(self, futures.FutureBase future)
    @cython.locals(dependencies=list)
    cpdef cancel_dependencies(self, error)

    cpdef _compute(self)
    cpdef _computed(self)

    cpdef _continue(self)
    cdef inline object _continue_on_generator(self, value, error)

    @cython.locals(scheduler=scheduler.TaskScheduler)
    cdef inline object _accept_yield_result(self, result)
    cdef inline object _accept_error(self, error)

    cpdef _queue_exit(self, result)
    cpdef _queue_throw_error(self, error)

    # Must be CPython method to bind:
    # cpdef _remove_dependency(self, dependency)
    cdef object _remove_dependency_cython(self, dependency)

    cpdef make_dependency(self, task, scheduler)

    cdef object _before_continue(self)
    cdef object _after_continue(self)

    cpdef _computed(self)
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

cpdef object _cancel_futures(object value, object error)

cdef tuple _empty_tuple
cdef dict _empty_dictionary
