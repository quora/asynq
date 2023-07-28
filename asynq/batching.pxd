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

cimport qcore.inspection as core_inspection
from . cimport futures
from . cimport _debug
from . cimport profiler


cdef _debug.DebugOptions _debug_options


cdef class BatchBase(futures.FutureBase):
    cdef public list items
    cdef public int _total_time
    cdef public int _id
    cdef dump_perf_stats(self, int time_taken)

    cpdef bint is_flushed(self) except -1
    cpdef bint is_cancelled(self) except -1
    cpdef bint is_empty(self) except -1
    cpdef object get_priority(self)

    cpdef flush(self)
    cpdef cancel(self, object error=?)
    cpdef to_str(self)

    cpdef _compute(self)
    cpdef _computed(self)
    cpdef _flush(self)
    cpdef _cancel(self)
    cpdef _try_switch_active_batch(self)

    cpdef dump(self, int indent=?)

cdef class BatchItemBase(futures.FutureBase):
    cdef public BatchBase batch
    cdef public long index
    cdef public int _total_time
    cdef public int _id

    cpdef _compute(self)
    cpdef to_str(self)

cdef class DebugBatchItem(BatchItemBase):
    cdef public object _result

cdef class DebugBatch(BatchBase):
    cdef public str name
    cdef public long index

    cpdef _flush(self)
    cpdef _cancel(self)


cdef object _debug_batch_state

cpdef sync(tag=?)
