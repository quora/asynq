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
from . cimport _debug

cdef _debug.DebugOptions _debug_options
cdef core_helpers.MarkerObject _none


@cython.auto_pickle(False)
cdef class FutureBase(object):
    cdef public object _value
    cdef public object _error
    cdef bint _in_repr
    cdef public core_events.EventHook on_computed

    cpdef object value(self)
    cpdef set_value(self, value)
    cpdef reset_unsafe(self)

    cpdef object error(self)
    cpdef set_error(self, error)

    cpdef bint is_computed(self) except -1
    cpdef _compute(self)
    cpdef _computed(self)

    cdef inline raise_if_error(self)

    cpdef dump(self, int indent=?)


@cython.auto_pickle(False)
cdef class Future(FutureBase):
    cdef public object _value_provider
    cpdef _compute(self)


@cython.auto_pickle(False)
cdef class ConstFuture(FutureBase):
    pass

@cython.auto_pickle(False)
cdef class ErrorFuture(FutureBase):
    pass
