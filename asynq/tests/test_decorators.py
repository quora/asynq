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

from qcore.asserts import assert_eq, assert_is, assert_is_instance, AssertRaises
from asynq import async, async_proxy, is_pure_async_fn, result, await, async_call, ConstFuture
from asynq.decorators import lazy, get_async_fn, get_async_or_sync_fn, make_async_decorator, AsyncDecorator
import pickle


def double_return_value(fun):
    @async(pure=True)
    def wrapper_fn(*args, **kwargs):
        value = yield fun.async(*args, **kwargs)
        result(value * 2); return
    return make_async_decorator(fun, wrapper_fn, 'double_return_value')


@double_return_value
@async()
def square(x):
    return x * x


class MyClass(object):
    @async()
    def method(self, number):
        assert type(self) is MyClass
        cls, one = self.get_cls_and_args(number)
        assert cls is MyClass
        assert one == number
        cls, one = yield self.get_cls_and_args.async(number)
        assert cls is MyClass
        assert one == number
        one = yield self.static(number)
        assert one == number
        one = yield self.static_ac.async(number)
        assert one == number
        cls, proxied = yield self.async_proxy_classmethod.async(number)
        assert cls is MyClass
        assert proxied == number
        result(self); return

    @async_proxy()
    @classmethod
    def async_proxy_classmethod(cls, number):
        return cls.get_cls_and_args.async(number)

    @async()
    @classmethod
    def get_cls_and_args(cls, number):
        print('get_cls_and_args')
        assert cls.get_cls_ac() is cls
        assert (yield cls.get_cls_ac.async()) is cls
        assert cls.get_cls().value() is cls
        assert (yield cls.get_cls()) is cls
        result((cls, number)); return

    @async()
    @classmethod
    def get_cls_ac(cls):
        print('get_cls_ac')
        result(cls); return

    @async(pure=True)
    @classmethod
    def get_cls(cls):
        print('get_cls')
        result(cls); return

    @async()
    @staticmethod
    def static_ac(number):
        print('static_ac')
        result(number); return

    @staticmethod
    @async(pure=True)
    def static(number):
        print('static')
        result(number); return

    @staticmethod
    def sync_staticmethod():
        return 'sync_staticmethod'

    @async(sync_fn=sync_staticmethod)
    @staticmethod
    def async_staticmethod():
        return 'async_staticmethod'

    @classmethod
    def sync_classmethod(cls):
        return 'sync_classmethod'

    @async(sync_fn=sync_classmethod)
    @classmethod
    def async_classmethod(cls):
        return 'async_classmethod'

    def sync_method(self):
        return 'sync_method'

    @async(sync_fn=sync_method)
    def async_method(self):
        return 'async_method'

    @double_return_value
    @async()
    @classmethod
    def square(cls, x):
        return x * x


def sync_fn():
    return 'sync_fn'


@async(sync_fn=sync_fn)
def async_fn():
    return 'async_fn'


@async(pure=True)
def pure_async_fn():
    return 'pure_async_fn'


def sync_proxied_fn():
    return 'sync_proxied_fn'


@async_proxy(sync_fn=sync_proxied_fn)
def async_proxied_fn():
    return ConstFuture('async_proxied_fn')


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
        raise AttributeError('cannot set attribute %s' % attr)


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
    assert_eq(async_fn.async, get_async_fn(async_fn))
    assert_eq(lazy_fn, get_async_fn(lazy_fn))
    assert_is(None, get_async_fn(sync_fn))

    wrapper = get_async_fn(sync_fn, wrap_if_none=True)
    assert is_pure_async_fn(wrapper)
    result = wrapper()
    assert_is_instance(result, ConstFuture)
    assert_eq('sync_fn', result.value())


def test_get_async_or_sync_fn():
    assert_is(sync_fn, get_async_or_sync_fn(sync_fn))
    assert_eq(async_fn.async, get_async_or_sync_fn(async_fn))


def test_async_proxy():
    assert_eq('sync_proxied_fn', sync_proxied_fn())
    assert_eq('sync_proxied_fn', async_proxied_fn())

    result = async_proxied_fn.async()
    assert_is_instance(result, ConstFuture)
    assert_eq('async_proxied_fn', result.value())

    with AssertRaises(AssertionError):
        @async_proxy(pure=True, sync_fn=sync_proxied_fn)
        def this_doesnt_make_sense():
            pass


def test():
    obj = MyClass()
    assert obj is obj.method(1)
    assert obj is await(obj.method.async(2))


def test_staticmethod_sync_fn():
    assert_eq('sync_staticmethod', MyClass.async_staticmethod())
    assert_eq('async_staticmethod', MyClass.async_staticmethod.async().value())


def test_classmethod_sync_fn():
    assert_eq('async_classmethod', MyClass.async_classmethod.async().value())
    assert_eq('sync_classmethod', MyClass.async_classmethod())


def test_method_sync_fn():
    instance = MyClass()
    assert_eq('sync_method', instance.async_method())
    assert_eq('async_method', instance.async_method.async().value())


def test_pickling():
    pickled = pickle.dumps(async_fn)
    unpickled = pickle.loads(pickled)
    assert_eq('sync_fn', unpickled())


def test_async_call():
    @async()
    def f1(arg, kw=1):
        return arg, kw

    @async(pure=True)
    def f2(arg, kw=1):
        return arg, kw

    def f3(arg, kw=1):
        return arg, kw

    for f in [f1, f2, f3]:
        assert_eq((10, 1), async_call.async(f, 10).value())
        assert_eq((10, 5), async_call.async(f, 10, 5).value())
        assert_eq((10, 7), async_call.async(f, 10, kw=7).value())


def test_make_async_decorator():
    assert_eq(18, square(3))
    assert_eq(18, MyClass.square(3))
    assert_eq(18, square.async(3).value())
    assert_eq(18, MyClass.square.async(3).value())

    assert not is_pure_async_fn(square)
    assert_eq('@double_return_value()', square.name())
