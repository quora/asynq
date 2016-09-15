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

cimport async_task


@cython.locals(active_task=async_task.AsyncTask)
cpdef async_task.AsyncTask enter_context(object context)
cpdef leave_context(object context, async_task.AsyncTask active_task)


cdef class NonAsyncContext(object):
    cpdef NonAsyncContext __enter__(self)
    cpdef __exit__(self, ty, val, tb)
    cpdef __pause__(self)
    cpdef __resume__(self)

cdef class AsyncContext(object):
    cdef public object _active_task
    cdef public bint _is_active
    cpdef AsyncContext __enter__(self)
    cpdef __exit__(self, ty, val, tb)
    cpdef __pause__(self)
    cpdef __resume__(self)
