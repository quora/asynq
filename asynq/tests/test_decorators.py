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
import pickle

from qcore.asserts import AssertRaises, assert_eq, assert_is, assert_is_instance

from asynq import ConstFuture, async_call, async_proxy, asynq, is_pure_async_fn
from asynq.decorators import (
    AsyncDecorator,
    get_async_fn,
    get_async_or_sync_fn,
    lazy,
    make_async_decorator,
)


def double_return_value(fun):
    @asynq(pure=True)
    def wrapper_fn(*args, **kwargs):
        value = yield fun.asynq(*args, **kwargs)
        return value * 2

    return make_async_decorator(fun, wrapper_fn, "double_return_value")


@double_return_value
@asynq()
def square(x):
    return x * x


class MyClass(object):
    @asynq()
    def method(self, number):
        assert type(self) is MyClass
        cls, one = self.get_cls_and_args(number)
        assert cls is MyClass
        assert one == number
        cls, one = yield self.get_cls_and_args.asynq(number)
        assert cls is MyClass
        assert one == number
        one = yield self.static(number)
        assert one == number
        one = yield self.static_ac.asynq(number)
        assert one == number
        cls, proxied = yield self.async_proxy_classmethod.asynq(number)
        assert cls is MyClass
        assert proxied == number
        return self

    @async_proxy()
    @classmethod
    def async_proxy_classmethod(cls, number):
        return cls.get_cls_and_args.asynq(number)

    @asynq()
    @classmethod
    def get_cls_and_args(cls, number):
        print("get_cls_and_args")
        assert cls.get_cls_ac() is cls
        assert (yield cls.get_cls_ac.asynq()) is cls
        assert cls.get_cls().value() is cls
        assert (yield cls.get_cls()) is cls
        return (cls, number)

    @asynq()
    @classmethod
    def get_cls_ac(cls):
        print("get_cls_ac")
        return cls

    @asynq(pure=True)
    @classmethod
    def get_cls(cls):
        print("get_cls")
        return cls

    @asynq()
    @staticmethod
    def static_ac(number):
        print("static_ac")
        return number

    @staticmethod
    @asynq(pure=True)
    def static(number):
        print("static")
        return number

    @staticmethod
    def sync_staticmethod():
        return "sync_staticmethod"

    @asynq(sync_fn=sync_staticmethod)
    @staticmethod
    def async_staticmethod():
        return "async_staticmethod"

    @classmethod
    def sync_classmethod(cls):
        return "sync_classmethod"

    @asynq(sync_fn=sync_classmethod)
    @classmethod
    def async_classmethod(cls):
        return "async_classmethod"

    def sync_method(self):
        return "sync_method"

    @asynq(sync_fn=sync_method)
    def async_method(self):
        return "async_method"

    @double_return_value
    @asynq()
    @classmethod
    def square(cls, x):
        return x * x


def sync_fn():
    return "sync_fn"


async def asyncio_fn():
    return "asyncio_fn"


@asynq(sync_fn=sync_fn, asyncio_fn=asyncio_fn)
def async_fn():
    return "async_fn"


@asynq(pure=True)
def pure_async_fn():
    return "pure_async_fn"


def sync_proxied_fn():
    return "sync_proxied_fn"


@async_proxy(sync_fn=sync_proxied_fn)
def async_proxied_fn():
    return ConstFuture("async_proxied_fn")


@lazy
def lazy_fn(a, b):
    return a + b


def test_lazy():
    future = lazy_fn(1, 2)
    assert not future.is_computed()
    assert_eq(3, future.value())
    assert future.is_computed()


class DisallowSetting(object):
    def fn(self):
        return False

    def __setattr__(self, attr, value):
        raise AttributeError("cannot set attribute %s" % attr)


def test_is_pure_async_fn():
    assert is_pure_async_fn(lazy_fn)
    assert not is_pure_async_fn(test_lazy)
    assert not is_pure_async_fn(async_fn)
    assert is_pure_async_fn(pure_async_fn)
    assert not is_pure_async_fn(DisallowSetting())
    assert is_pure_async_fn(MyClass.get_cls)
    assert not is_pure_async_fn(MyClass.get_cls_ac)
    assert not is_pure_async_fn(AsyncDecorator)


def test_get_async_fn():
    assert_eq(async_fn.asynq, get_async_fn(async_fn))
    assert_eq(lazy_fn, get_async_fn(lazy_fn))
    assert_is(None, get_async_fn(sync_fn))

    wrapper = get_async_fn(sync_fn, wrap_if_none=True)
    assert is_pure_async_fn(wrapper)
    result = wrapper()
    assert_is_instance(result, ConstFuture)
    assert_eq("sync_fn", result.value())


def test_get_async_or_sync_fn():
    assert_is(sync_fn, get_async_or_sync_fn(sync_fn))
    assert_eq(async_fn.asynq, get_async_or_sync_fn(async_fn))


def test_async_proxy():
    assert_eq("sync_proxied_fn", sync_proxied_fn())
    assert_eq("sync_proxied_fn", async_proxied_fn())

    result = async_proxied_fn.asynq()
    assert_is_instance(result, ConstFuture)
    assert_eq("async_proxied_fn", result.value())

    with AssertRaises(AssertionError):

        @async_proxy(pure=True, sync_fn=sync_proxied_fn)
        def this_doesnt_make_sense():
            pass


def test():
    obj = MyClass()
    assert obj is obj.method(1)


def test_staticmethod_sync_fn():
    assert_eq("sync_staticmethod", MyClass.async_staticmethod())
    assert_eq("async_staticmethod", MyClass.async_staticmethod.asynq().value())


def test_classmethod_sync_fn():
    assert_eq("async_classmethod", MyClass.async_classmethod.asynq().value())
    assert_eq("sync_classmethod", MyClass.async_classmethod())


def test_method_sync_fn():
    instance = MyClass()
    assert_eq("sync_method", instance.async_method())
    assert_eq("async_method", instance.async_method.asynq().value())


def test_function_sync_asyncio_fn():
    assert_eq("sync_fn", async_fn())
    assert_eq("async_fn", async_fn.asynq().value())
    assert_eq("asyncio_fn", asyncio.run(async_fn.asyncio()))


def test_pickling():
    pickled = pickle.dumps(async_fn)
    unpickled = pickle.loads(pickled)
    assert_eq("sync_fn", unpickled())


def test_async_call():
    @asynq()
    def f1(arg, kw=1):
        return arg, kw

    @asynq(pure=True)
    def f2(arg, kw=1):
        return arg, kw

    def f3(arg, kw=1):
        return arg, kw

    for f in [f1, f2, f3]:
        assert_eq((10, 1), async_call.asynq(f, 10).value())
        assert_eq((10, 5), async_call.asynq(f, 10, 5).value())
        assert_eq((10, 7), async_call.asynq(f, 10, kw=7).value())

    @asynq()
    def g0(f, *args, **kwargs):
        d = yield async_call.asynq(f, *args, **kwargs)
        return d

    for f in [f1, f2, f3]:
        assert_eq((10, 1), asyncio.run(async_call.asyncio(f, 10)))
        assert_eq((10, 1), asyncio.run(g0.asyncio(f, 10)))
        assert_eq((10, 5), asyncio.run(g0.asyncio(f, 10, 5)))
        assert_eq((10, 7), asyncio.run(g0.asyncio(f, 10, kw=7)))


def test_make_async_decorator():
    assert_eq(18, square(3))
    assert_eq(18, MyClass.square(3))
    assert_eq(18, square.asynq(3).value())
    assert_eq(18, MyClass.square.asynq(3).value())

    assert not is_pure_async_fn(square)
    assert_eq("@double_return_value()", square.name())
