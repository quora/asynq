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

import sys
import time

from asynq import asynq, AsyncContext, result
from asynq.tools import (
    amap,
    afilter,
    afilterfalse,
    amin,
    amax,
    asift,
    asorted,
    acached_per_instance,
    call_with_context,
    deduplicate,
    AsyncTimer,
    AsyncEventHook,
)
from qcore.asserts import assert_eq, assert_gt, assert_is, AssertRaises, assert_unordered_list_eq


@asynq()
def inner_fn(x):
    pass


@asynq()
def filter_fn(elt):
    yield inner_fn.asynq(elt)
    result(elt is not None); return


@asynq()
def alen(seq):
    return len(seq)


def test_afilter():
    assert_eq([], list(afilter.asynq(filter_fn, []).value()))
    assert_eq([], list(afilter.asynq(filter_fn, [None]).value()))
    assert_eq([1], list(afilter(filter_fn, [None, 1, None])))
    assert_eq([1], list(afilter.asynq(filter_fn, [None, 1, None]).value()))
    assert_eq([1], list(afilter(None, [None, 1, None])))


def test_afilterfalse():
    assert_eq([], list(afilterfalse.asynq(filter_fn, []).value()))
    assert_eq([None], list(afilterfalse.asynq(filter_fn, [None]).value()))
    assert_eq([None, None], list(afilterfalse(filter_fn, [None, 1, None])))
    assert_eq([None, None], list(afilterfalse.asynq(filter_fn, [None, 1, None]).value()))


def test_asift():
    assert_eq(([], []), asift.asynq(filter_fn, []).value())
    assert_eq(([], [None]), asift.asynq(filter_fn, [None]).value())
    assert_eq(([1], [None, None]), asift(filter_fn, [None, 1, None]))
    assert_eq(([1], [None, None]), asift.asynq(filter_fn, [None, 1, None]).value())


def test_amap():
    assert_eq([False], list(amap(filter_fn, [None])))
    assert_eq([True], list(amap(filter_fn, [4])))
    assert_eq([], list(amap(filter_fn, [])))
    assert_eq([False, True, False], list(amap(filter_fn, [None, '', None])))


def test_asorted():
    assert_eq([], asorted([], key=filter_fn))
    assert_eq([None], asorted([None], key=filter_fn))
    assert_eq([None, True], asorted([True, None], key=filter_fn))
    assert_eq([1, 2], asorted([2, 1]))


def test_amax():
    assert_eq(1, amax(1, None, key=filter_fn))
    assert_eq(1, amax([1, None], key=filter_fn))
    assert_eq(1, amax((elt for elt in (1, None)), key=filter_fn))
    assert_eq([1, 2, 3], amax([1], [1, 2, 3], [1, 2], key=alen))
    assert_eq([1, 2, 3], amax([[1], [1, 2, 3], [1, 2]], key=alen))

    assert_eq(4, amax(1, 2, 3, 4))

    with AssertRaises(TypeError):
        amax(key=filter_fn)
    with AssertRaises(ValueError):
        amax([], key=filter_fn)
    with AssertRaises(TypeError):
        amax([], key=filter_fn, random_keyword_argument='raising a TypeError')


def test_amin():
    assert_is(None, amin(1, None, key=filter_fn))
    assert_is(None, amin([1, None], key=filter_fn))
    assert_is(None, amin((elt for elt in (1, None)), key=filter_fn))
    assert_eq([1], amin([1], [1, 2, 3], [1, 2], key=alen))
    assert_eq([1], amin([[1], [1, 2, 3], [1, 2]], key=alen))

    assert_eq(1, amin(1, 2, 3, 4))

    with AssertRaises(TypeError):
        amin(key=filter_fn)
    with AssertRaises(ValueError):
        amin([], key=filter_fn)
    with AssertRaises(TypeError):
        amin([], key=filter_fn, random_keyword_argument='raising a TypeError')


class AsyncObject(object):
    cls_value = 0

    def __init__(self):
        self.value = 0

    @acached_per_instance()
    @asynq()
    def get_value(self, index):
        self.value += 1
        return self.value

    @acached_per_instance()
    @asynq()
    def with_kwargs(self, x=1, y=2, z=3):
        self.value += (x + y + z)
        return self.value

    if sys.version_info >= (3, 0):
        exec("""
@acached_per_instance()
@asynq()
def with_kwonly_arg(self, *, arg=1):
    return arg
""")

    @deduplicate()
    @asynq()
    def increment_value_method(self, val=1):
        self.value += val

    @deduplicate()
    @asynq()
    @staticmethod
    def deduplicated_static_method(val=1):
        AsyncObject.cls_value += val


class UnhashableAcached(AsyncObject):
    __hash__ = None


def test_acached_per_instance():
    for cls in (AsyncObject, UnhashableAcached):
        obj = cls()
        cache = type(obj).get_value.decorator.__acached_per_instance_cache__
        assert_eq(0, len(cache), extra=repr(cache))

        assert_eq(1, obj.get_value(0))
        assert_eq(1, obj.get_value(0))
        assert_eq(2, obj.get_value(1))
        assert_eq(1, obj.get_value(0))
        assert_eq(1, obj.get_value(index=0))
        assert_eq(1, obj.get_value.asynq(index=0).value())

        assert_eq(8, obj.with_kwargs())
        assert_eq(8, obj.with_kwargs(z=3))
        assert_eq(17, obj.with_kwargs(x=3, y=3))

        assert_eq(1, len(cache), extra=repr(cache))

        if sys.version_info >= (3, 0):
            assert_eq(1, obj.with_kwonly_arg(arg=1))

        del obj
        assert_eq(0, len(cache), extra=repr(cache))


class Ctx(AsyncContext):
    is_on = False

    def pause(self):
        Ctx.is_on = False

    def resume(self):
        Ctx.is_on = True


@asynq()
def assert_state(value):
    yield AsyncObject().get_value.asynq(value)
    assert_is(value, Ctx.is_on)


def test_call_with_context():
    assert_state(False)
    call_with_context(Ctx(), assert_state, True)

i = 0


@deduplicate()
@asynq()
def increment_value(val=1):
    global i
    i += val


@deduplicate()
@asynq()
def recursive_incrementer(n):
    if n == 0:
        result((yield increment_value.asynq(n))); return
    result(recursive_incrementer(n - 1)); return


@deduplicate()
@asynq()
def call_with_dirty():
    call_with_dirty.dirty()


@deduplicate()
@asynq()
def recursive_call_with_dirty():
    global i
    if i > 0:
        result(i); return
    i += 1
    recursive_call_with_dirty.dirty()
    yield recursive_call_with_dirty.asynq()


if sys.version_info >= (3, 0):
    exec("""
@deduplicate()
@asynq()
def call_with_kwonly_arg(*, arg):
    return arg
""")


def test_deduplicate():
    _check_deduplicate()


@asynq()
def _check_deduplicate():
    global i
    i = 0
    AsyncObject.cls_value = 0

    yield increment_value.asynq()
    assert_eq(1, i)

    yield increment_value.asynq(), increment_value.asynq(1)
    assert_eq(2, i)

    obj = AsyncObject()
    yield obj.increment_value_method.asynq(), obj.increment_value_method.asynq(1)
    assert_eq(1, obj.value)

    yield AsyncObject.deduplicated_static_method.asynq(), \
        AsyncObject.deduplicated_static_method.asynq(1)
    assert_eq(1, AsyncObject.cls_value)

    i = 0
    yield recursive_call_with_dirty.asynq()

    yield call_with_dirty.async()

    if sys.version_info >= (3, 0):
        with AssertRaises(TypeError):
            yield call_with_kwonly_arg.async(1)
        assert_eq(1, (yield call_with_kwonly_arg.async(arg=1)))


def test_deduplicate_recursion():
    _check_deduplicate_recursion()


@asynq()
def _check_deduplicate_recursion():
    yield recursive_incrementer.asynq(20), increment_value.asynq(0)


def test_async_timer():
    _check_async_timer()


@asynq()
def _slow_task(t):
    yield None
    time.sleep(t)
    result(0); return


@asynq()
def _timed_slow_task(t):
    with AsyncTimer() as timer:
        yield None
        time.sleep(t)
    result(timer.total_time); return


@asynq()
def _check_async_timer():
    with AsyncTimer() as t:
        results = yield [_slow_task.asynq(0.1), _timed_slow_task.asynq(0.1),
                         _slow_task.asynq(0.1), _timed_slow_task.asynq(0.1)]
        assert_eq(0, results[0])
        assert_eq(105000, results[1], tolerance=5000)
        assert_eq(0, results[0])
        assert_eq(105000, results[3], tolerance=5000)

    assert_eq(210000, sum(results), tolerance=10000)
    assert_eq(410000, t.total_time, tolerance=10000)
    assert_gt(t.total_time, sum(results))


def test_async_event_hook():
    calls = []
    @asynq()
    def handler1(*args):
        assert_gt(len(args), 0)
        calls.append('handler1%s' % str(args))

    def handler2(*args):
        calls.append('handler2%s' % str(args))

    hook = AsyncEventHook([handler1])
    hook.subscribe(handler2)

    # trigger
    hook.trigger(1, 2, 'a')
    assert_unordered_list_eq(['handler1(1, 2, \'a\')', 'handler2(1, 2, \'a\')'], calls)

    calls = []
    @asynq()
    def async_trigger():
        yield hook.trigger.asynq(2,3)

    async_trigger()
    assert_unordered_list_eq(['handler1(2, 3)', 'handler2(2, 3)'], calls)

    # safe_trigger
    calls = []
    hook2 = AsyncEventHook([handler1, handler2])
    # calling it with no args will raise AssertionError in handler1
    with AssertRaises(AssertionError):
        hook2.safe_trigger()
    assert_eq(['handler2()'], calls)

    # make sure that the order doesn't matter
    calls = []
    hook3 = AsyncEventHook([handler2, handler1])
    # calling it with no args will raise AssertionError in handler1
    with AssertRaises(AssertionError):
        hook3.safe_trigger()
    assert_eq(['handler2()'], calls)
