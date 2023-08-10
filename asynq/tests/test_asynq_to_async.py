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

import asynq


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
