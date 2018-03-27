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
from asynq.py3.decorators import async, async_proxy, async_call, get_async_fn, AsyncDecorator
from asynq.py3.batching import ConstFuture
from asynq import is_pure_async_fn
from asynq.decorators import get_async_or_sync_fn
import pickle


class MyClass(object):
    @async()
    def method(self, number):
        assert type(self) is MyClass
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
        return self

    @async_proxy()
    @classmethod
    def async_proxy_classmethod(cls, number):
        return cls.get_cls_and_args.async(number)

    @async()
    @classmethod
    def get_cls_and_args(cls, number):
        print('get_cls_and_args')
        assert (yield cls.get_cls_ac.async()) is cls
        assert (yield cls.get_cls()) is cls
        return (cls, number)

    @async()
    @classmethod
    def get_cls_ac(cls):
        print('get_cls_ac')
        return cls

    @async(pure=True)
    @classmethod
    def get_cls(cls):
        print('get_cls')
        return cls

    @async()
    @staticmethod
    def static_ac(number):
        print('static_ac')
        return number

    @staticmethod
    @async(pure=True)
    def static(number):
        print('static')
        return number


@async(pure=True)
def pure_async_fn():
    return 'pure_async_fn'


@async_proxy()
def async_proxied_fn():
    return ConstFuture('async_proxied_fn')


@async()
def async_fn():
    return 'async_fn'


def sync_fn():
    return 'sync_fn'


class DisallowSetting(object):
    def fn(self):
        return False

    def __setattr__(self, attr, value):
        raise AttributeError('cannot set attribute %s' % attr)


def test_is_pure_async_fn():
    assert not is_pure_async_fn(async_fn)
    assert is_pure_async_fn(pure_async_fn)
    assert not is_pure_async_fn(DisallowSetting())
    assert is_pure_async_fn(MyClass.get_cls)
    assert not is_pure_async_fn(MyClass.get_cls_ac)
    assert not is_pure_async_fn(AsyncDecorator)


def test_get_async_fn():
    assert_eq(async_fn.async, get_async_fn(async_fn))
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
    assert_eq('async_proxied_fn', async_proxied_fn())

    result = async_proxied_fn.async()
    assert_eq('async_proxied_fn', result.value())


def test():
    obj = MyClass()
    assert obj is obj.method(1)


def test_pickling():
    pickled = pickle.dumps(async_fn)
    unpickled = pickle.loads(pickled)
    assert_eq('async_fn', unpickled())


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
