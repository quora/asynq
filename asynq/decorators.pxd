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

cimport qcore.helpers as core_helpers
cimport qcore.inspection as core_inspection
cimport qcore.decorators
cimport futures
cimport async_task
cimport _debug

cdef _debug.DebugOptions _debug_options


# cpdef lazy(object fn)
cpdef bint has_async_fn(object fn) except -1
cpdef bint is_pure_async_fn(object fn) except -1


cdef class PureAsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    cpdef bint is_pure_async_fn(self) except -1


cdef class PureAsyncDecorator(qcore.decorators.DecoratorBase):
    cdef public type task_cls
    cdef public dict kwargs
    cdef public bint needs_wrapper

    cpdef str name(self)
    cpdef bint is_pure_async_fn(self) except -1
    cpdef object _call_pure(self, tuple args, dict kwargs)


cdef class AsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    pass


cdef class AsyncDecorator(PureAsyncDecorator):
    pass


cdef class AsyncAndSyncPairDecorator(AsyncDecorator):
    cdef public object sync_fn


cdef class AsyncProxyDecorator(AsyncDecorator):
    pass


cdef class AsyncAndSyncPairProxyDecorator(AsyncProxyDecorator):
    pass

