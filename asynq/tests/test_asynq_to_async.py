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


import asyncio
import time

from qcore.asserts import assert_eq

import asynq
from asynq import ConstFuture
from asynq.tools import AsyncTimer, deduplicate


def test_asyncio():
    async def f3():
        return 200

    @asynq.asynq(asyncio_fn=f3)
    def f2():
        return 100

    @asynq.asynq()
    def f(x):
        a = yield asynq.ConstFuture(3)
        b = yield asynq.ConstFuture(2)
        assert (yield None) is None
        return a - b + x

    @asynq.asynq()
    def g(x):
        obj = yield {
            "a": [f.asynq(0), f.asynq(1)],
            "b": (f.asynq(2), f.asynq(3)),
            "c": f.asynq(4),
            "d": f2.asynq(),
        }
        return obj

    assert asyncio.run(g.asyncio(5)) == {"a": [1, 2], "b": (3, 4), "c": 5, "d": 200}


def test_context():
    async def blocking_op():
        await asyncio.sleep(0.1)

    @asynq.asynq()
    def f1():
        with AsyncTimer() as timer:
            time.sleep(0.1)
            t1, t2 = yield f2.asynq()
            time.sleep(0.1)
        return timer.total_time, t1, t2

    @asynq.asynq()
    def f2():
        with AsyncTimer() as timer:
            time.sleep(0.1)
            t = yield f3.asynq()
            time.sleep(0.1)
        return timer.total_time, t

    @asynq.asynq()
    def f3():
        with AsyncTimer() as timer:
            # since AsyncTimer is paused on blocking operations,
            # the time for TestBatch is not measured
            yield [blocking_op(), blocking_op()]
        return timer.total_time

    t1, t2, t3 = asyncio.run(f1.asyncio())
    assert_eq(400000, t1, tolerance=10000)  # 400ms, 10us tolerance
    assert_eq(200000, t2, tolerance=10000)  # 200ms, 10us tolerance
    assert_eq(000000, t3, tolerance=10000)  #   0ms, 10us tolerance


def test_method():
    async def g(slf, x):
        return slf._x + x + 20

    class A:
        def __init__(self, x):
            self._x = x

        @asynq.asynq(asyncio_fn=g)
        def f(self, x):
            return self._x + x + 10

    a = A(100)
    assert_eq(a.f(5), 115)

    @asynq.asynq()
    def original(x):
        return (yield a.f.asynq(x))

    assert_eq(original(6), 116)
    assert_eq(asyncio.run(a.f.asyncio(7)), 127)


def test_pure():
    @asynq.asynq(pure=True)
    def h():
        return 100

    @asynq.asynq()
    def i():
        return (yield h())

    assert i() == 100
    assert asyncio.run(i.asyncio()) == 100


def test_proxy():
    async def k(x):
        return x + 999

    @asynq.async_proxy(asyncio_fn=k)
    def j(x):
        return ConstFuture(x + 888)

    assert j(-100) == 788
    assert j.asynq(-200).value() == 688
    assert asyncio.run(j.asyncio(-300)) == 699


def test_deduplicate():
    @deduplicate()
    @asynq.asynq()
    def l():
        return 3

    async def n():
        return 4

    @deduplicate()
    @asynq.asynq(asyncio_fn=n)
    def m():
        return 3

    assert l() == 3
    assert asyncio.run(l.asyncio()) == 3

    assert m() == 3
    assert asyncio.run(m.asyncio()) == 4
