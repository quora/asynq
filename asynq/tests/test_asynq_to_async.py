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
from asynq.tools import AsyncTimer
from asynq.batching import BatchItemBase, BatchBase


async def f3():
    return 200


@asynq.asynq(asyncio_fn=f3)
def f2():
    return 100


@asynq.asynq()
def f(x):
    a = yield asynq.ConstFuture(3)
    b = yield asynq.ConstFuture(2)
    assert (yield None) == None
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


def test_asyncio():
    assert asyncio.run(g.asyncio(5)) == {"a": [1, 2], "b": (3, 4), "c": 5, "d": 200}




def test_context():
    class TestBatchItem(BatchItemBase):
        pass

    class TestBatch(BatchBase):
        def _try_switch_active_batch(self):
            pass
        def _flush(self):
            for item in self.items:
                item.set_value(None)
            time.sleep(0.1 * len(self.items))
        def _cancel(self):
            pass

    batch = TestBatch()

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
            yield [TestBatchItem(batch), TestBatchItem(batch)]
        return timer.total_time

    t1, t2, t3 = f1()
    assert_eq(400000, t1, tolerance=10000)  # 400ms, 10us tolerance
    assert_eq(200000, t2, tolerance=10000)  # 200ms, 10us tolerance
    assert_eq(000000, t3, tolerance=10000)  #   0ms, 10us tolerance
