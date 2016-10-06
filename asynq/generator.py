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

__doc__ = """

Async generators.

"""
import qcore
import functools

from .decorators import async, async_proxy
from .futures import ConstFuture
from .utils import result

END_OF_GENERATOR = qcore.MarkerObject(u'end of generator')


def async_generator():
    """Decorator to create an async generator.

    async functions are always implemented as generators, but it is sometimes useful to yield values
    (as in a normal, non-async generator) from an async function. This decorator provides that
    capability. Inside of the generator, wrap any data (as opposed to async futures) you want to
    yield in a Value object. For example:

    >>> @async()
    ... def async_function():
    ...     return 42
    >>> @async_generator()
    ... def gen():
    ...     value = yield async_function.async()
    ...     yield Value(value)
    >>> list_of_generator(gen())
    >>> [42]

    Async generators cannot function exactly like normal generators, because to do so their .next
    method would have to be invoked asynchronously, which is not possible in Python 2. (Python 3
    adds an async iteration protocol.) Therefore, async generators produce asynq Futures instead of
    producing their values directly. You should normally iterate over an async generator with code
    like:

        for task in generator():
            value = yield task
            # do stuff with value

    Additionally, generators that have an async yield after the last Value they yield will produce
    an additional value, END_OF_GENERATOR, that should be ignored by calling code. To guard against
    this possibility, you need to write:

        for task in generator():
            value = yield task
            if value is END_OF_GENERATOR:
                continue
            # do stuff with value

    """
    def decorator(fun):
        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            gen = fun(*args, **kwargs)
            return _AsyncGenerator(gen)
        return wrapped
    return decorator


class Value(object):
    """Represents a value yielded by an async generator."""
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return '<Value: %r>' % self.value


@async()
def list_of_generator(generator):
    """Returns a list of all the values in the async generator."""
    data = []
    for task in generator:
        value = yield task
        if value is END_OF_GENERATOR:
            continue
        data.append(value)
    result(data); return


@async()
def take_first(generator, n):
    """Returns the first n values in the generator."""
    ret = []
    for i, task in enumerate(generator):
        value = yield task
        if value is END_OF_GENERATOR:
            continue
        ret.append(value)
        if i == n - 1:
            break
    result(ret); return


class _AsyncGenerator(object):
    def __init__(self, generator):
        self.generator = generator
        self.last_task = None
        self.is_stopped = False

    def __iter__(self):
        return self

    @async_proxy(pure=True)
    def next(self):
        return self.send.async(None)

    __next__ = next  # Python 3

    @async_proxy()
    def send(self, value):
        # ensure that the previous yielded value has been computed
        if self.last_task is not None:
            if not self.last_task.is_computed():
                raise RuntimeError(
                    'You must compute the previous task before advancing the generator')
        if self.is_stopped:
            raise StopIteration

        # get a first value so that we can raise StopIteration here immediately
        # this works only if there are no async yields after the last Value is yielded; otherwise
        # the generator will yield END_OF_GENERATOR before being exhausted
        first_value = self._get_one_value(value)
        if isinstance(first_value, Value):
            return ConstFuture(first_value.value)
        task = self._send_inner.async(first_value)
        self.last_task = task
        return task

    @async()
    def _send_inner(self, first_task):
        # ensure previous item has been consumed
        yield_result = yield first_task
        while True:
            try:
                value = self._get_one_value(yield_result)
            except StopIteration:
                result(END_OF_GENERATOR); return
            if isinstance(value, Value):
                result(value.value); return
            else:
                yield_result = yield value

    def _get_one_value(self, value):
        try:
            return self.generator.send(value)
        except StopIteration:
            self.is_stopped = True
            raise

    def __repr__(self):
        return '<@async_generator() %s %s>' % (self.generator, 'stopped' if self.stopped else '')
