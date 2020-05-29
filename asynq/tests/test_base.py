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

from asynq import asynq, Future, scheduler

values = {}  # type: ignore


@asynq(pure=True)
def get(key):
    global values
    value = values.get(key)
    print("Get %s -> %s" % (str(key), str(value)))
    return value


@asynq(pure=True)
def set(key, value):
    global values
    values[key] = value
    print("Set %s <- %s" % (str(key), str(value)))


@asynq(pure=True)
def get_and_set(key_from, key_to, depends_on):
    yield depends_on
    value = yield get(key_from)
    yield set(key_to, value)


@asynq(pure=True)
def order_test():
    global values
    values = {}
    prev_task = set(0, "value")
    tasks = []
    for i in range(0, 10):
        task = get_and_set(i, i + 1, prev_task)  # No need to yield!
        prev_task = task
        tasks.append(task)

    assert len(values) == 0  # Nothing is executed yet!

    yield tasks
    assert len(values) == 11  # Done at this point

    # Nothing happens here
    yield None
    assert len(values) == 11  # Done at this point


def test():
    order_test()()
