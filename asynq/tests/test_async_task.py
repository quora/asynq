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
from asynq.futures import ConstFuture
from asynq.contexts import AsyncContext
from asynq import scheduler

from qcore.asserts import assert_eq, assert_is, AssertRaises


def check_unwrap(expected, to_unwrap):
    # can't call unwrap() directly because it's hidden by cython
    @asynq()
    def caller():
        value = yield to_unwrap
        assert_eq(expected, value, extra="yielded {}".format(to_unwrap))

    caller()


def test_unwrap():
    check_unwrap(None, None)
    check_unwrap(1, ConstFuture(1))

    for typ in (list, tuple):
        check_unwrap(typ(), typ())
        check_unwrap(typ([1]), typ([ConstFuture(1)]))
        check_unwrap(typ([1, 2]), typ([ConstFuture(1), ConstFuture(2)]))
        check_unwrap(
            typ([1, 2, 3]), typ([ConstFuture(1), ConstFuture(2), ConstFuture(3)])
        )

    with AssertRaises(TypeError):
        check_unwrap(1)


class Ctx(AsyncContext):
    def __init__(self, ctx_id):
        self.ctx_id = ctx_id

    def __eq__(self, other):
        return self.ctx_id == other.ctx_id

    def resume(self):
        pass

    def pause(self):
        pass


def test_context_nesting():
    """Make sure that context nesting ordering is preserved.

    This should be true even when two contexts are __eq__ to one another.

    """

    @asynq()
    def fn():
        task = scheduler.get_active_task()
        ctx0 = Ctx(0)
        ctx1 = Ctx(1)
        ctx2 = Ctx(0)
        with ctx0:
            with ctx1:
                with ctx2:
                    assert_eq([ctx0, ctx1, ctx2], list(task._contexts.values()))

                assert_eq([ctx0, ctx1], list(task._contexts.values()))

    fn()


def test_clear_dependencies():
    """Ensure that we the list of dependencies won't continue growing for a long-running task."""

    @asynq()
    def inner_fn():
        return None

    @asynq()
    def fn():
        task = scheduler.get_active_task()
        assert_eq(0, len(task._dependencies))
        yield [inner_fn.asynq() for _ in range(10)]
        assert_eq(0, len(task._dependencies))

    fn()
