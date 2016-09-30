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
from cpython cimport bool

from futures cimport FutureBase
cimport async_task
cimport scheduler
cimport _debug


cdef _debug.DebugOptions _debug_options

@cython.locals(task=async_task.AsyncTask, current_scheduler=scheduler.TaskScheduler, new_scheduler=scheduler.TaskScheduler)
cpdef object execute_on_separate_scheduler(object async_pull_fn, list contexts, tuple args=?, dict kwargs=?)

cdef inline object _await(tuple args)

@cython.locals(l=int)
cdef inline object _unwrap(tuple args)

cpdef result(object value)

# Private part

cdef _extract_tasks(object value, set result)

