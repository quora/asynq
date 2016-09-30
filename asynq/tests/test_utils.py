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

from asynq import async, await, result, ConstFuture, AsyncTaskResult, AsyncContext
from asynq.utils import execute_on_separate_scheduler
from qcore.asserts import assert_eq, assert_is, AssertRaises


class Context(AsyncContext):
    def __init__(self):
        self._is_active = False
        self.state = "pause"

    def resume(self):
        self.state = "resume"

    def pause(self):
        self.state = "pause"


def test_execute_on_separate_scheduler():
    ctx1 = Context()
    ctx2 = Context()
    ctx3 = Context()

    def inner(arg1, arg2=''):
        assert_eq('resume', ctx1.state)
        assert_eq('resume', ctx2.state)
        assert_eq('pause', ctx3.state)
        yield
        result(arg1 + arg2); return

    @async()
    def outer():
        with ctx3:
            pass

        with ctx2:
            value = execute_on_separate_scheduler(inner, [ctx1, ctx2], args=('hello',))
            assert_eq('hello', value)
            value = execute_on_separate_scheduler(inner, [ctx1, ctx2], args=('hello',),
                                                  kwargs={'arg2': ' world'})
            assert_eq('hello world', value)

    outer()


@async()
def f(x):
    return x * x


@async()
def g():
    res = yield
    assert_is(None, res)

    res = yield f.async(3)
    assert_eq(9, res)

    for sequence_length in range(6):
        res = yield tuple(f.async(i) for i in range(sequence_length))
        expected_tuple = tuple(i * i for i in range(sequence_length))
        assert_eq(expected_tuple, res)

        res = yield [f.async(i) for i in range(sequence_length)]
        assert_eq(list(expected_tuple), res)

        res = yield {i: f.async(i) for i in range(sequence_length)}
        expected_dict = {i: i * i for i in range(sequence_length)}
        assert_eq(expected_dict, res)


def test_unwrapping():
    g()


def test_result():
    with AssertRaises(AssertionError):
        result(ConstFuture(None))
    with AssertRaises(AsyncTaskResult):
        result(None)


def test_await():
    assert_is(None, await(None))
    assert_eq(9, await(f.async(3)))
    assert_eq((4, 9), await(f.async(2), f.async(3)))
    assert_eq((1, [4, 9]), await(f.async(1), [f.async(2), f.async(3)]))
    assert_eq(6, await(ConstFuture(6)))
    assert_eq({1: 2, 3: 4}, await({1: ConstFuture(2), 3: ConstFuture(4)}))

    with AssertRaises(TypeError):
        await(42)
