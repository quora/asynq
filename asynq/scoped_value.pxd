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

cimport contexts


cdef object _empty_context

cdef class AsyncScopedValue(object):
    cpdef public object _value

    cpdef object get(self)
    cpdef set(self, object value)
    cpdef object override(self, object value)

cdef class _AsyncScopedValueOverrideContext(contexts.AsyncContext):
    cdef AsyncScopedValue _target
    cdef object _value
    cdef object _old_value

    cpdef resume(self)
    cpdef pause(self)


cdef class _AsyncPropertyOverrideContext(contexts.AsyncContext):
    cdef object _target
    cdef object _property_name
    cdef object _value
    cdef object _old_value

    cpdef resume(self)
    cpdef pause(self)

cdef object async_override  # Alias of AsyncPropertyOverrideContext
