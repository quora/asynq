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

from asynq import AsyncContext, async, await, debug, result, NonAsyncContext
from .helpers import Profiler
from qcore.asserts import assert_eq, assert_is, AssertRaises

from . import model


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
            assert_eq('pause', self.state)
        self.state = "resume"
        current_context = self
        print(self.name + ': resume')

    def pause(self):
        global current_context
        if self.assert_state_changes:
            assert_eq('resume', self.state)
        self.state = "pause"
        current_context = self.parent
        print(self.name + ': pause')


def test_parallel():
    @async(pure=True)
    def parallel(name, parent, level):
        with Context(name, parent) as ctx:
            if level >= 2:
                yield debug.sync()
            else:
                yield (
                    parallel(name + '-1', ctx, level + 1),
                    parallel(name + '-2', ctx, level + 1),
                )

    with Profiler('test_parallel()'):
        await(
            parallel('taskA', None, 0),
            parallel('taskB', None, 0)
        )
    print()


def test_adder():
    global change_amount
    change_amount = 0

    @async(pure=True)
    def async_add(a, b):
        yield debug.sync()
        z = a + b + change_amount
        result(z); return

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

    @async(pure=True)
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
        result((yield async_add(z, q))); return

    @async(pure=True)
    def useless():
        a, b, c = yield (add_twice(1, 1), add_twice(1, 1), async_add(1, 1))
        result((a, b, c)); return

    assert_eq(2, async_add(1, 1)())
    assert_eq((7, 7, 2), await(add_twice(1, 1), add_twice(1, 1), async_add(1, 1)))
    assert_eq((7, 7, 2), useless()())

    expected_change_amount_base += 1
    with AsyncAddChanger(1):
        assert_eq((10, 10, 3), await(add_twice(1, 1), add_twice(1, 1), async_add(1, 1)))


def test_context_binding_on_construction():
    global current_context

    @async(pure=True)
    def nested(ctx):
        assert_is(current_context, ctx)
        yield

    @async(pure=True)
    def outermost():
        assert_is(None, current_context)
        with Context('ctx', None, assert_state_changes=False) as ctx:
            assert_is(current_context, ctx)
            task = nested(ctx)  # Active async contexts are bound to task on construction

        assert_is(None, current_context)
        assert_eq(1, len(task._contexts))
        assert_is(ctx, task._contexts[0])  # The ctx is captured
        result((yield task)); return

    outermost()()


class Ctx(NonAsyncContext):
    def __enter__(self):
        super(Ctx, self).__enter__()
        return self

    def __exit__(self, typ, val, tb):
        super(Ctx, self).__exit__(typ, val, tb)
        return


def test_non_async_context():
    @async()
    def async_fn_with_yield(arg, should_yield):
        with Ctx():
            if should_yield:
                user = yield model.get_user(arg)
            else:
                user = 'user ' + arg
        result(user); return

    @async()
    def batch(should_yield=True):
        user1, user2 = yield async_fn_with_yield.async('1', should_yield), \
            async_fn_with_yield.async('2', should_yield)
        result((user1, user2)); return

    with AssertRaises(AssertionError):
        batch()

    # assert that without the yields, there is not AssertionError
    batch(False)
