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

from asynq import AsyncTask, async
from asynq.generator import END_OF_GENERATOR, async_generator, list_of_generator, take_first, Value
from qcore.asserts import assert_eq, assert_is, assert_is_instance, AssertRaises


def test_value():
    val = Value('value')
    assert_eq('value', val.value)
    assert_eq("<Value: 'value'>", repr(val))


@async()
def alen(seq):
    return len(seq)


@async_generator()
def generator():
    for value in ([], [1], [1, 2]):
        length = yield alen.async(value)
        yield Value(length)


@async_generator()
def generator_with_more_yields():
    for task in generator():
        value = yield task
        value = yield alen.async([value] * value)
        yield Value(value)
    yield alen.async([1, 2])


@async_generator()
def generator_without_yields():
    for i in range(3):
        yield Value(i)


def test_list_of_generator():
    assert_eq([0, 1, 2], list_of_generator(generator()))
    assert_eq([0, 1, 2], list_of_generator(generator_with_more_yields()))


def test_take_first():
    gen = generator()
    assert_eq([0], take_first(gen, 1))
    assert_eq([1, 2], take_first(gen, 2))
    assert_eq([], take_first(gen, 3))

    gen = generator_with_more_yields()
    assert_eq([0, 1, 2], take_first(gen, 4))

    gen = generator_without_yields()
    assert_eq([0, 1, 2], take_first(gen, 4))


def test_must_compute():
    gen = generator()
    next(gen)
    with AssertRaises(RuntimeError):
        next(gen)


def test_values():
    gen = generator_with_more_yields()
    for i in range(3):
        task = next(gen)
        assert_is_instance(task, AsyncTask)
        assert_eq(i, task.value())

    task = next(gen)
    assert_is_instance(task, AsyncTask)
    assert_is(END_OF_GENERATOR, task.value())

    with AssertRaises(StopIteration):
        next(gen)
