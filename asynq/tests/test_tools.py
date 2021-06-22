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

import time

from asynq import asynq, AsyncContext, scheduler
from asynq.tools import (
    amap,
    afilter,
    afilterfalse,
    amin,
    amax,
    asift,
    asorted,
    acached_per_instance,
    alru_cache,
    alazy_constant,
    aretry,
    call_with_context,
    deduplicate,
    AsyncTimer,
    AsyncEventHook,
)
from qcore.asserts import (
    assert_eq,
    assert_gt,
    assert_is,
    AssertRaises,
    assert_unordered_list_eq,
)
from qcore import get_original_fn
import inspect
import pickle
from unittest import mock


@asynq()
def inner_fn(x):
    pass


@asynq()
def filter_fn(elt):
    yield inner_fn.asynq(elt)
    return elt is not None


@asynq()
def alen(seq):
    return len(seq)


def gen():
    yield 1
    yield None


def test_afilter():
    assert_eq([], afilter.asynq(filter_fn, []).value())
    assert_eq([], afilter.asynq(filter_fn, [None]).value())
    assert_eq([1], afilter(filter_fn, [None, 1, None]))
    assert_eq([1], afilter.asynq(filter_fn, [None, 1, None]).value())
    assert_eq([1], afilter(None, [None, 1, None]))
    assert_eq([1], afilter(filter_fn, gen()))
    assert_eq([1], afilter(None, gen()))


def test_afilterfalse():
    assert_eq([], afilterfalse.asynq(filter_fn, []).value())
    assert_eq([None], afilterfalse.asynq(filter_fn, [None]).value())
    assert_eq([None, None], afilterfalse(filter_fn, [None, 1, None]))
    assert_eq([None, None], afilterfalse.asynq(filter_fn, [None, 1, None]).value())
    assert_eq([None], afilterfalse(filter_fn, gen()))


def test_asift():
    assert_eq(([], []), asift.asynq(filter_fn, []).value())
    assert_eq(([], [None]), asift.asynq(filter_fn, [None]).value())
    assert_eq(([1], [None, None]), asift(filter_fn, [None, 1, None]))
    assert_eq(([1], [None, None]), asift.asynq(filter_fn, [None, 1, None]).value())


def test_amap():
    assert_eq([False], list(amap(filter_fn, [None])))
    assert_eq([True], list(amap(filter_fn, [4])))
    assert_eq([], list(amap(filter_fn, [])))
    assert_eq([False, True, False], list(amap(filter_fn, [None, "", None])))


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
        amax([], key=filter_fn, random_keyword_argument="raising a TypeError")


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
        amin([], key=filter_fn, random_keyword_argument="raising a TypeError")


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
        self.value += x + y + z
        return self.value

    @acached_per_instance()
    @asynq()
    def raises_exception(self):
        assert False

    @acached_per_instance()
    @asynq()
    def with_kwonly_arg(self, *, arg=1):
        return arg

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
    __hash__ = None  # type: ignore


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

        assert_eq(1, obj.with_kwonly_arg(arg=1))

        del obj
        assert_eq(0, len(cache), extra=repr(cache))


def test_acached_per_instance_exception_handling():
    obj = AsyncObject()
    try:
        obj.raises_exception()
    except AssertionError:
        # the exception should not affect the internals of the scheduler, and the active task
        # should get cleaned up
        assert_is(None, scheduler.get_active_task())


def test_alru_cache():
    _check_alru_cache()


@asynq()
def _check_alru_cache():
    @alru_cache(maxsize=1, key_fn=lambda args, kwargs: args[0] % 2 == 0)
    @asynq()
    def cube(n):
        return n * n * n

    assert_eq(1, (yield cube.asynq(1)))
    # hit the cache
    assert_eq(1, (yield cube.asynq(3)))
    # cache miss
    assert_eq(8, (yield cube.asynq(2)))
    # now it's a cache miss
    assert_eq(27, (yield cube.asynq(3)))


def test_alazy_constant():
    _check_alazy_constant_no_ttl()
    _check_alazy_constant_ttl()


constant_call_count = 0


@asynq()
def _check_alazy_constant_no_ttl():
    global constant_call_count
    constant_call_count = 0

    @alazy_constant()
    @asynq()
    def constant():
        global constant_call_count
        constant_call_count += 1
        return constant_call_count

    # multiple calls in a short time should only call it once
    assert_eq(1, (yield constant.asynq()))
    assert_eq(1, (yield constant.asynq()))
    assert_eq(1, (yield constant.asynq()))

    # but after a dirty, it should be called again
    constant.dirty()
    assert_eq(2, (yield constant.asynq()))
    assert_eq(2, (yield constant.asynq()))
    assert_eq(2, (yield constant.asynq()))


@asynq()
def _check_alazy_constant_ttl():
    global constant_call_count
    constant_call_count = 0

    @alazy_constant(ttl=100000)  # 100ms
    @asynq()
    def constant():
        global constant_call_count
        constant_call_count += 1
        return constant_call_count

    # multiple calls in a short time should only call it once
    assert_eq(1, (yield constant.asynq()))
    assert_eq(1, (yield constant.asynq()))
    assert_eq(1, (yield constant.asynq()))

    # but after a long enough time, it should be called again
    time.sleep(0.1)
    assert_eq(2, (yield constant.asynq()))
    assert_eq(2, (yield constant.asynq()))
    assert_eq(2, (yield constant.asynq()))

    # or after a dirty
    constant.dirty()
    assert_eq(3, (yield constant.asynq()))
    assert_eq(3, (yield constant.asynq()))
    assert_eq(3, (yield constant.asynq()))


class AnyException(Exception):
    pass


class AnyOtherException(Exception):
    pass


@aretry(Exception)
@asynq()
def retry_it():
    pass


class TestRetry(object):
    def create_function(self, exception_type, max_tries):
        fn_body = mock.Mock()
        fn_body.return_value = []

        @aretry(exception_type, max_tries=max_tries)
        @asynq()
        def function(*args, **kwargs):
            return fn_body(*args, **kwargs)

        return function, fn_body

    def test_pickling(self):
        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            pickled = pickle.dumps(retry_it, protocol=protocol)
            assert_is(retry_it, pickle.loads(pickled))

    def test_retry_passes_all_arguments(self):
        function, fn_body = self.create_function(AnyException, max_tries=2)
        function(1, 2, foo=3)
        fn_body.assert_called_once_with(1, 2, foo=3)

    def test_retry_does_not_retry_on_no_exception(self):
        function, fn_body = self.create_function(AnyException, max_tries=3)
        function()
        fn_body.assert_called_once_with()

    def test_retry_does_not_retry_on_unspecified_exception(self):
        function, fn_body = self.create_function(AnyException, max_tries=3)
        fn_body.side_effect = AnyOtherException

        with AssertRaises(AnyOtherException):
            function()

        fn_body.assert_called_once_with()

    def test_retry_retries_on_provided_exception(self):
        max_tries = 4
        function, fn_body = self.create_function(AnyException, max_tries)
        fn_body.side_effect = AnyException

        with AssertRaises(AnyException):
            function()

        assert_eq(max_tries, fn_body.call_count)

    def test_retry_requires_max_try_at_least_one(self):
        with AssertRaises(Exception):
            self.create_function(AnyException, max_tries=0)
        self.create_function(AnyException, max_tries=1)

    def test_retry_can_take_multiple_exceptions(self):
        max_tries = 4

        expected_exceptions = (AnyException, AnyOtherException)

        function, fn_body = self.create_function(expected_exceptions, max_tries)
        fn_body.side_effect = AnyException

        with AssertRaises(AnyException):
            function()

        assert_eq(max_tries, fn_body.call_count)
        fn_body.reset_mock()

        fn_body.side_effect = AnyOtherException

        with AssertRaises(AnyOtherException):
            function()

        assert_eq(max_tries, fn_body.call_count)

    def test_retry_preserves_argspec(self):
        def fn(foo, bar, baz=None, **kwargs):
            pass

        decorated = aretry(Exception)(fn)

        assert_eq(
            inspect.getargspec(fn), inspect.getargspec(get_original_fn(decorated))
        )


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
        return (yield increment_value.asynq(n))
    return recursive_incrementer(n - 1)


@deduplicate()
@asynq()
def call_with_dirty():
    call_with_dirty.dirty()


@deduplicate()
@asynq()
def recursive_call_with_dirty():
    global i
    if i > 0:
        return i
    i += 1
    recursive_call_with_dirty.dirty()
    yield recursive_call_with_dirty.asynq()


@asynq()
def dummy():
    pass


@asynq()
def deduplicate_caller():
    yield deduplicated_recusive.asynq()


@deduplicate()
@asynq()
def deduplicated_recusive():
    global i
    existing = i
    i = 1
    yield dummy.asynq()
    if existing == 0:
        deduplicate_caller()


@deduplicate()
@asynq()
def call_with_kwonly_arg(*, arg):
    return arg


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

    yield AsyncObject.deduplicated_static_method.asynq(), AsyncObject.deduplicated_static_method.asynq(
        1
    )
    assert_eq(1, AsyncObject.cls_value)

    i = 0
    yield recursive_call_with_dirty.asynq()

    yield call_with_dirty.asynq()

    with AssertRaises(TypeError):
        yield call_with_kwonly_arg.asynq(1)
    assert_eq(1, (yield call_with_kwonly_arg.asynq(arg=1)))

    i = 0
    deduplicate_caller()


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
    return 0


@asynq()
def _timed_slow_task(t):
    with AsyncTimer() as timer:
        yield None
        time.sleep(t)
    return timer.total_time


@asynq()
def _check_async_timer():
    with AsyncTimer() as t:
        results = yield [
            _slow_task.asynq(0.1),
            _timed_slow_task.asynq(0.1),
            _slow_task.asynq(0.1),
            _timed_slow_task.asynq(0.1),
        ]
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
        calls.append("handler1%s" % str(args))

    def handler2(*args):
        calls.append("handler2%s" % str(args))

    hook = AsyncEventHook([handler1])
    hook.subscribe(handler2)

    # trigger
    hook.trigger(1, 2, "a")
    assert_unordered_list_eq(["handler1(1, 2, 'a')", "handler2(1, 2, 'a')"], calls)

    calls = []

    @asynq()
    def async_trigger():
        yield hook.trigger.asynq(2, 3)

    async_trigger()
    assert_unordered_list_eq(["handler1(2, 3)", "handler2(2, 3)"], calls)

    # safe_trigger
    calls = []
    hook2 = AsyncEventHook([handler1, handler2])
    # calling it with no args will raise AssertionError in handler1
    with AssertRaises(AssertionError):
        hook2.safe_trigger()
    assert_eq(["handler2()"], calls)

    # make sure that the order doesn't matter
    calls = []
    hook3 = AsyncEventHook([handler2, handler1])
    # calling it with no args will raise AssertionError in handler1
    with AssertRaises(AssertionError):
        hook3.safe_trigger()
    assert_eq(["handler2()"], calls)


class DeduplicateClassWrapper:
    @deduplicate()
    @asynq()
    def return_three(self):
        return 3

    @deduplicate()
    @asynq()
    def return_five(self):
        return 5

    @asynq()
    def return_three_and_five(self):
        return (yield (self.return_three.asynq(), self.return_five.asynq()))


def test_deduplicate_same_class():
    obj = DeduplicateClassWrapper()

    # make sure the five method has a separate key and therefore there was no cache mixup
    assert_eq((3, 5), obj.return_three_and_five())
