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


cdef class DebugOptions(object):
    cdef public bint DUMP_PRE_ERROR_STATE
    cdef public bint DUMP_EXCEPTIONS
    cdef public bint DUMP_SCHEDULE_TASK
    cdef public bint DUMP_CONTINUE_TASK
    cdef public bint DUMP_SCHEDULE_BATCH
    cdef public bint DUMP_FLUSH_BATCH
    cdef public bint DUMP_DEPENDENCIES
    cdef public bint DUMP_COMPUTED
    cdef public bint DUMP_NEW_TASKS
    cdef public bint DUMP_YIELD_RESULTS
    cdef public bint DUMP_QUEUED_RESULTS
    cdef public bint DUMP_CONTEXTS
    cdef public bint DUMP_SYNC
    cdef public bint DUMP_STACK
    cdef public bint DUMP_SCHEDULER_STATE
    cdef public bint DUMP_SYNC_CALLS
    cdef public bint COLLECT_PERF_STATS

    cdef public float SCHEDULER_STATE_DUMP_INTERVAL
    cdef public int DEBUG_STR_REPR_MAX_LENGTH
    cdef public object STACK_DUMP_LIMIT
    cdef public int MAX_TASK_STACK_SIZE

    cdef public object ENABLE_COMPLEX_ASSERTIONS
    cdef public object KEEP_DEPENDENCIES

    cpdef DUMP_ALL(self, value=?)

cdef public DebugOptions options
