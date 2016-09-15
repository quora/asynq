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

import inspect
from types import FunctionType

import qcore.helpers as core_helpers
import qcore.inspection as core_inspection
import qcore.decorators

from . import futures
from . import async_task
from . import _debug

__traceback_hide__ = True

_debug_options = _debug.options


def lazy(fn):
    """Converts a function into a lazy one - i.e. its call
    returns a Future bound to a function call with passed
    arguments, that can be evaluated on demand later

    """
    @core_inspection.wraps(fn)
    def new_fn(*args, **kwargs):
        value_provider = lambda: fn(*args, **kwargs)
        return futures.Future(value_provider)

    new_fn.is_pure_async_fn = core_helpers.true_fn
    return new_fn


def has_async_fn(fn):
    """Returns true if fn has .async member."""
    return hasattr(fn, 'async')


def is_pure_async_fn(fn):
    """Returns true if fn is an @async(pure=True) or @async_proxy(pure=True) function."""
    if hasattr(fn, 'is_pure_async_fn'):
        try:
            return fn.is_pure_async_fn()
        except TypeError:
            # this happens when calling e.g. is_pure_async_fn(AsyncDecorator)
            return False
    if hasattr(fn, 'fn'):
        result = is_pure_async_fn(fn.fn)
        result_fn = core_helpers.true_fn if result else core_helpers.false_fn
        try:
            fn.is_pure_async_fn = result_fn
        except (TypeError, AttributeError):
            pass  # some callables don't let you assign attributes
        return result
    return False


def is_async_fn(fn):
    """Returns true if fn is an @async([pure=True]) or @async_proxy(pure=True) function."""
    return hasattr(fn, 'async') or is_pure_async_fn(fn)


def get_async_fn(fn, wrap_if_none=False):
    """Returns an async function for the specified source function."""
    if hasattr(fn, 'async'):
        return fn.async
    if is_pure_async_fn(fn):
        return fn
    if wrap_if_none:
        def sync_to_async_fn_wrapper(*args, **kwargs):
            return futures.ConstFuture(fn(*args, **kwargs))

        sync_to_async_fn_wrapper.is_pure_async_fn = core_helpers.true_fn
        return sync_to_async_fn_wrapper
    return None


def get_async_or_sync_fn(fn):
    """Returns an async function for the specified fn, if it exists; otherwise returns source."""
    if hasattr(fn, 'async'):
        return fn.async
    return fn


class PureAsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    def is_pure_async_fn(self):
        return True


class PureAsyncDecorator(qcore.decorators.DecoratorBase):
    binder_cls = PureAsyncDecoratorBinder

    def __init__(self, fn, task_cls, kwargs={}):
        qcore.decorators.DecoratorBase.__init__(self, fn)
        self.task_cls = task_cls
        self.needs_wrapper = core_inspection.is_cython_or_generator(fn)
        self.kwargs = kwargs

    def name(self):
        return '@async(pure=True)'

    def is_pure_async_fn(self):
        return True

    def _fn_wrapper(self, args, kwargs):
        raise async_task.AsyncTaskResult(self.fn(*args, **kwargs)); return
        yield

    def __call__(self, *args, **kwargs):
        return self._call_pure(args, kwargs)

    def _call_pure(self, args, kwargs):
        if not self.needs_wrapper:
            result = self._fn_wrapper(args, kwargs)
        else:
            result = self.fn(*args, **kwargs)
        return self.task_cls(result, self.fn, args, kwargs, **self.kwargs)


class AsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    def async(self, *args, **kwargs):
        if self.instance is None:
            return self.decorator.async(*args, **kwargs)
        else:
            return self.decorator.async(self.instance, *args, **kwargs)


class AsyncDecorator(PureAsyncDecorator):
    binder_cls = AsyncDecoratorBinder

    def is_pure_async_fn(self):
        return False

    def async(self, *args, **kwargs):
        return self._call_pure(args, kwargs)

    def name(self):
        return '@async()'

    def __call__(self, *args, **kwargs):
        return self._call_pure(args, kwargs).value()


class AsyncAndSyncPairDecoratorBinder(AsyncDecoratorBinder):
    def __call__(self, *args, **kwargs):
        # the base class implementation adds .instance here, but we don't want that because we
        # called __get__ on the sync_fn manually; if we don't do this we'll end up adding self or
        # cls twice
        return self.decorator(*args, **kwargs)


class AsyncAndSyncPairDecorator(AsyncDecorator):
    binder_cls = AsyncAndSyncPairDecoratorBinder

    def __init__(self, fn, cls, sync_fn, kwargs={}):
        AsyncDecorator.__init__(self, fn, cls, kwargs)
        self.sync_fn = sync_fn

    def __call__(self, *args, **kwargs):
        return self.sync_fn(*args, **kwargs)

    def __get__(self, owner, cls):
        # This is needed so that we can use objects with __get__ as the sync_fn. If we just rely on
        # the base class's __get__ implementation, we'll never end up calling __get__ on the
        # sync_fn, so if it's a method of some sort it will not be bound correctly. We get around
        # this by manually calling __get__ on the sync_fn, then creating a copy of ourselves with
        # the bound sync_fn and calling the base class's __get__ implementation on it.
        sync_fn = self.sync_fn.__get__(owner, cls)
        fn = self.fn
        if self.type in (staticmethod, classmethod):
            fn = self.type(fn)
        new_self = qcore.decorators.decorate(
            AsyncAndSyncPairDecorator, self.task_cls, sync_fn, self.kwargs,
        )(fn)
        return AsyncDecorator.__get__(new_self, owner, cls)


class AsyncProxyDecorator(AsyncDecorator):
    def __init__(self, fn):
        # we don't need the task class but still need to pass it to the superclass
        AsyncDecorator.__init__(self, fn, None)

    def _call_pure(self, args, kwargs):
        return self.fn(*args, **kwargs)


class AsyncAndSyncPairProxyDecorator(AsyncProxyDecorator):
    def __init__(self, fn, sync_fn):
        AsyncProxyDecorator.__init__(self, fn)
        self.sync_fn = sync_fn

    def __call__(self, *args, **kwargs):
        return self.sync_fn(*args, **kwargs)


def async(pure=False, sync_fn=None, cls=async_task.AsyncTask, **kwargs):
    """Async task decorator.
    Converts a method returning generator object to
    a method returning AsyncTask object.

    """
    if kwargs:
        assert pure, "custom kwargs are only supported with pure=True"
    if pure:
        assert sync_fn is None, "sync_fn is not supported for pure async functions"

    def decorate(fn):
        assert not (is_pure_async_fn(fn) or has_async_fn(fn)), \
            "@async() decorator can be applied just once"
        if pure:
            return qcore.decorators.decorate(PureAsyncDecorator, cls, kwargs)(fn)
        elif sync_fn is None:
            return qcore.decorators.decorate(AsyncDecorator, cls)(fn)
        else:
            return qcore.decorators.decorate(AsyncAndSyncPairDecorator, cls, sync_fn)(fn)

    return decorate


def async_proxy(pure=False, sync_fn=None):
    if sync_fn is not None:
        assert pure is False, "sync_fn=? cannot be used together with pure=True"

    def decorate(fn):
        if pure:
            return fn
        if sync_fn is None:
            return qcore.decorators.decorate(AsyncProxyDecorator)(fn)
        else:
            return qcore.decorators.decorate(AsyncAndSyncPairProxyDecorator, sync_fn)(fn)

    return decorate


def cached(cache):
    """Caching decorator. Caches method execution result
    based on its name an arguments.
    * Works for both async and non-async methods
    * Result is always async method returning AsyncTask object
    * Provides dirty(...) member allowing to dirty appropriate cache entry.

    """
    _assert_apply_without_invocation("async", (cache,))

    def decorate(fn):
        original_fn = core_inspection.get_original_fn(fn)

        @core_inspection.wraps(fn)
        def new_fn(*args, **kwargs):
            return _cached(cache, original_fn, fn, args, kwargs)

        def dirty(*args, **kwargs):
            key = _get_cache_key(original_fn, args, kwargs)
            cache.set(key, core_helpers.miss)

        new_fn.dirty = dirty
        new_fn.fn = fn
        new_fn.is_pure_async_fn = core_helpers.true_fn

        return new_fn
    return decorate


@async_proxy()
def async_call(fn, *args, **kwargs):
    """Use this if you are not sure if fn is async or not.

    e.g. when you are within an async function and you need to call fn but it could either be
    async or non-async, you should write
    val = yield async_call.async(fn, arg1, kw1=value1)

    """
    if is_pure_async_fn(fn):
        return fn(*args, **kwargs)
    if is_async_fn(fn):
        return fn.async(*args, **kwargs)
    return futures.ConstFuture(fn(*args, **kwargs))


class AsyncWrapper(qcore.decorators.DecoratorBase):
    """Implements make_async_decorator."""
    binder_cls = AsyncDecoratorBinder

    def __init__(self, fn, wrapper_fn, name):
        super(AsyncWrapper, self).__init__(fn)
        self.wrapper_fn = wrapper_fn
        self.function_name = name

    def name(self):
        return '@%s()' % self.function_name

    def _call_async(self, args, kwargs):
        return self.wrapper_fn(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self._call_async(args, kwargs).value()

    def async(self, *args, **kwargs):
        return self._call_async(args, kwargs)

    def is_pure_async_fn(self):
        return False


def make_async_decorator(fn, wrapper_fn, name):
    """For implementing decorators that wrap async functions.

    Arguments:
    - fn: the function to be wrapped
    - wrapper_fn: function used to implement the decorator. Must return a Future.
    - name: name of the decorator (used for introspection)

    """
    return qcore.decorators.decorate(
        AsyncWrapper,
        wrapper_fn,
        name,
    )(fn)


# Private part

def _assert_apply_without_invocation(decorator, task_args, task_kwargs={}):
    if not task_kwargs and len(task_args) == 1:
        assert not isinstance(task_args[0], FunctionType), "you must use @%s(...) instead of @%s" % (decorator, decorator)


def _cached_impl(cache, original_fn, fn, args, kwargs):
    """Internal async method used by @cached decorator.
    Implements cache check/update in async fashion.

    """
    key = _get_cache_key(original_fn, args, kwargs)
    cached = yield cache.get(key)
    if cached is not core_helpers.miss:
        raise async_task.AsyncTaskResult(cached); return
    else:
        result = fn(*args, **kwargs)
        if isinstance(result, futures.FutureBase):
            result = yield result
        _cache_set(cache, key, result).start()
        raise async_task.AsyncTaskResult(result); return

# Applying decorator by this way to make it work with Cython
_cached = async(pure=True)(_cached_impl)
globals()['_cached'] = _cached


def _cache_set_impl(cache, key, value):
    yield cache.set(key, value)
    raise StopIteration()

# Applying decorator by this way to make it work with Cython
_cache_set = async(pure=True)(_cache_set_impl)
globals()['_cache_set'] = _cache_set


def _get_cache_key(fn, args, kwargs):
    return core_inspection.get_function_call_repr(fn, args, kwargs)
