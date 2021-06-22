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

from asynq import AsyncContext, asynq, debug, NonAsyncContext
from .helpers import Profiler
from qcore.asserts import assert_eq, assert_is, AssertRaises

from .debug_cache import mc
from .caching import ExternalCacheBatchItem


current_context = None
change_amount = 0


class Context(AsyncContext):
    def __init__(self, name, parent, assert_state_changes=True):
        self.name = name
        self.parent = parent
        self.state = "pause"
        self.assert_state_changes = assert_state_changes

    def __enter__(self):
        global current_context
        assert_is(self.parent, current_context)
        return super(Context, self).__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        global current_context
        assert_is(self, current_context)
        super(Context, self).__exit__(exc_type, exc_val, exc_tb)

    def resume(self):
        global current_context
        if self.assert_state_changes:
            assert_eq("pause", self.state)
        self.state = "resume"
        current_context = self
        print(self.name + ": resume")

    def pause(self):
        global current_context
        if self.assert_state_changes:
            assert_eq("resume", self.state)
        self.state = "pause"
        current_context = self.parent
        print(self.name + ": pause")


def test_parallel():
    @asynq(pure=True)
    def parallel(name, parent, level):
        with Context(name, parent) as ctx:
            if level >= 2:
                yield debug.sync()
            else:
                yield (
                    parallel(name + "-1", ctx, level + 1),
                    parallel(name + "-2", ctx, level + 1),
                )

    with Profiler("test_parallel()"):

        @asynq()
        def together():
            yield parallel("taskA", None, 0), parallel("taskB", None, 0)

        together()
    print()


def test_adder():
    global change_amount
    change_amount = 0

    @asynq(pure=True)
    def async_add(a, b):
        yield debug.sync()
        z = a + b + change_amount
        return z

    class AsyncAddChanger(AsyncContext):
        def __init__(self, diff):
            self.diff = diff

        def resume(self):
            global change_amount
            change_amount += self.diff

        def pause(self):
            global change_amount
            change_amount -= self.diff

    global expected_change_amount_base
    expected_change_amount_base = 0

    @asynq(pure=True)
    def add_twice(a, b):
        global change_amount
        global expected_change_amount_base
        assert_eq(expected_change_amount_base, change_amount)
        with AsyncAddChanger(1):
            assert_eq(expected_change_amount_base + 1, change_amount)
            z = yield async_add(a, b)
            assert_eq(expected_change_amount_base + 1, change_amount)
            with AsyncAddChanger(1):
                q = yield async_add(a, b)
                assert_eq(expected_change_amount_base + 2, change_amount)
            assert_eq(expected_change_amount_base + 1, change_amount)
        assert_eq(expected_change_amount_base + 0, change_amount)
        return (yield async_add(z, q))

    @asynq(pure=True)
    def useless():
        a, b, c = yield (add_twice(1, 1), add_twice(1, 1), async_add(1, 1))
        return (a, b, c)

    assert_eq(2, async_add(1, 1)())
    assert_eq((7, 7, 2), useless()())

    expected_change_amount_base += 1
    with AsyncAddChanger(1):
        assert_eq((10, 10, 3), useless()())


class Ctx(NonAsyncContext):
    def __enter__(self):
        super().__enter__()
        return self

    def __exit__(self, typ, val, tb):
        super().__exit__(typ, val, tb)


def test_non_async_context():
    @asynq()
    def async_fn_with_yield(should_yield):
        with Ctx():
            if should_yield:
                ret = yield ExternalCacheBatchItem(mc._batch, "get", "test")
            else:
                ret = 0
        return ret

    @asynq()
    def batch(should_yield=True):
        ret1, ret2 = yield (
            async_fn_with_yield.asynq(should_yield),
            async_fn_with_yield.asynq(should_yield),
        )
        return (ret1, ret2)

    with AssertRaises(AssertionError):
        batch()

    # assert that without the yields, there is not AssertionError
    batch(False)
