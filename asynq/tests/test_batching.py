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

from asynq import asynq
from .debug_cache import reset_caches, mc
from .caching import ExternalCacheBatchItem


def test_chain():
    @asynq()
    def foo(num_yield):
        if num_yield == 0:
            return 0

        yield ExternalCacheBatchItem(mc._batch, "get", "test")
        yield foo.asynq(num_yield - 1)

    reset_caches()
    foo(10)
    assert mc._batch.index == 11


def test_tree():
    @asynq()
    def foo(depth):
        if depth == 0:
            return (yield ExternalCacheBatchItem(mc._batch, "get", "test"))
        yield foo.asynq(depth - 1), foo.asynq(depth - 1)

    reset_caches()
    foo(5)
    assert mc._batch.index == 2
