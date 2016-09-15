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

import gc

from asynq import async, debug, result, AsyncTask
from .helpers import Profiler

values = {}


class WrappedAsyncTask(AsyncTask):
    pass


def wrapped_async(*args, **kwargs):
    return async(*args, cls=WrappedAsyncTask, **kwargs)


# async = decorators.async_old
# async = wrapped_async


@async(pure=True)
def get(key):
    global values
    result(values.get(key)); return
    yield  # Must be a generator


@async(pure=True)
def set(key, value):
    global values
    values[key] = value
    return
    yield  # Must be a generator


@async(pure=True)
def get_and_set(key_from, key_to):
    value = yield get(key_from)
    yield set(key_to, value)


@async(pure=True)
def performance_test(task_count):
    global values
    values = {}
    assert len(values) == 0  # Nothing is executed yet!

    yield set(0, 0)
    assert len(values) == 1

    yield [get_and_set(i, i + 1) for i in range(0, task_count)]
    assert len(values) == task_count + 1  # Done at this point


def test():
    with Profiler('test_performance(100): warming up'):
        performance_test(100).value()
    gc.collect()
    with Profiler('test_performance(3000): actual test (w/assertions)'):
        performance_test(3000).value()
    gc.collect()
    with debug.disable_complex_assertions(), \
         Profiler('test_performance(3000): actual test (w/o assertions)'):
        performance_test(3000).value()
