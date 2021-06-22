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

from qcore import MarkerObject
from asynq import asynq, async_proxy, ConstFuture
from collections import deque


# TODO(alex): finish w/this test


empty = MarkerObject("empty @ asynq.channels")
future_empty = ConstFuture(empty)
future_false = ConstFuture(False)
future_true = ConstFuture(True)


class Channel(object):
    def __init__(self, capacity=256):
        self.capacity = capacity
        self.items = deque()
        self.futures = deque()

    @async_proxy(pure=True)
    def push(self, value, should_await=True):
        if self.futures:
            future = self.futures.popleft()
            future.set_value(value)
            return future_true
        if len(self.items) < self.capacity:
            self.items.append(value)
            return future_true
        return _push_async(self, value) if should_await else future_false

    @async_proxy(pure=True)
    def pull(self, should_await=True):
        if self.items:
            return ConstFuture(self.items.popleft())
        return _pull_async(self) if should_await else future_empty


@asynq(pure=True)
def _push_async(channel, value):
    yield None
    while True:
        if channel.push(value, False) is future_true:
            return future_true
        yield None


@asynq(pure=True)
def _pull_async(channel):
    yield None
    while True:
        item = channel.pull(False)
        if item is not future_empty:
            return item
        yield None
