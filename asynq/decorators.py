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
import inspect
from typing import Any, Coroutine

import qcore.decorators
import qcore.helpers as core_helpers
import qcore.inspection as core_inspection

from . import async_task, futures
from .asynq_to_async import AsyncioMode, is_asyncio_mode, resolve_awaitables

__traceback_hide__ = True


def lazy(fn):
    """Converts a function into a lazy one - i.e. its call
    returns a Future bound to a function call with passed
    arguments, that can be evaluated on demand later

    """

    @core_inspection.wraps(fn)
    def new_fn(*args, **kwargs):
        def value_provider():
            return fn(*args, **kwargs)

        return futures.Future(value_provider)

    new_fn.is_pure_async_fn = core_helpers.true_fn
    return new_fn


def has_async_fn(fn):
    """Returns true if fn can be called asynchronously."""
    return hasattr(fn, "async") or hasattr(fn, "asynq")


def is_pure_async_fn(fn):
    """Returns true if fn is an @asynq(pure=True) or @async_proxy(pure=True) function."""
    if hasattr(fn, "is_pure_async_fn"):
        try:
            return fn.is_pure_async_fn()
        except (IndexError, TypeError):
            # This happens when calling e.g. is_pure_async_fn(AsyncDecorator). TypeError is the
            # expected exception in this case, but with some versions of Cython IndexError is thrown
            # instead.
            return False
    if hasattr(fn, "fn"):
        result = is_pure_async_fn(fn.fn)
        result_fn = core_helpers.true_fn if result else core_helpers.false_fn
        try:
            fn.is_pure_async_fn = result_fn
        except (TypeError, AttributeError):
            pass  # some callables don't let you assign attributes
        return result
    return False


def is_async_fn(fn):
    """Returns true if fn is an @asynq([pure=True]) or @async_proxy(pure=True) function."""
    return hasattr(fn, "asynq") or hasattr(fn, "async") or is_pure_async_fn(fn)


def get_async_fn(fn, wrap_if_none=False):
    """Returns an async function for the specified source function."""
    if hasattr(fn, "asynq"):
        return fn.asynq
    if hasattr(fn, "async"):
        return getattr(fn, "async")
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
    if hasattr(fn, "asynq"):
        return fn.asynq
    if hasattr(fn, "async"):
        return getattr(fn, "async")
    return fn


def convert_asynq_to_async(fn):
    if inspect.isgeneratorfunction(fn):

        async def wrapped(*_args, **_kwargs):
            task = asyncio.current_task()
            with AsyncioMode():
                send, exception = None, None

                generator = fn(*_args, **_kwargs)
                while True:
                    try:
                        if exception is None:
                            result = generator.send(send)
                        else:
                            result = generator.throw(
                                type(exception), exception, exception.__traceback__
                            )
                    except StopIteration as exc:
                        return exc.value

                    try:
                        send = await resolve_awaitables(result)
                        exception = None
                    except Exception as exc:
                        exception = exc

        return wrapped
    else:

        async def wrapped(*_args, **_kwargs):
            with AsyncioMode():
                return fn(*_args, **_kwargs)

        return wrapped


class PureAsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    def is_pure_async_fn(self):
        return True


class PureAsyncDecorator(qcore.decorators.DecoratorBase):
    binder_cls = PureAsyncDecoratorBinder

    def __init__(self, fn, task_cls, kwargs={}, asyncio_fn=None):
        qcore.decorators.DecoratorBase.__init__(self, fn)
        self.task_cls = task_cls
        self.needs_wrapper = core_inspection.is_cython_or_generator(fn)
        self.kwargs = kwargs
        self.asyncio_fn = asyncio_fn

    def name(self):
        return "@asynq(pure=True)"

    def is_pure_async_fn(self):
        return True

    def _fn_wrapper(self, args, kwargs):
        raise async_task.AsyncTaskResult(self.fn(*args, **kwargs))
        return
        yield

    def asyncio(self, *args, **kwargs) -> Coroutine[Any, Any, Any]:
        if self.asyncio_fn is None:
            self.asyncio_fn = convert_asynq_to_async(self.fn)

        return self.asyncio_fn(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self._call_pure(args, kwargs)

    def _call_pure(self, args, kwargs):
        if is_asyncio_mode():
            return self.asyncio(*args, **kwargs)

        if not self.needs_wrapper:
            result = self._fn_wrapper(args, kwargs)
        else:
            result = self.fn(*args, **kwargs)
        return self.task_cls(result, self.fn, args, kwargs, **self.kwargs)


class AsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    def asynq(self, *args, **kwargs):
        if self.instance is None:
            return self.decorator.asynq(*args, **kwargs)
        else:
            return self.decorator.asynq(self.instance, *args, **kwargs)

    def asyncio(self, *args, **kwargs) -> Coroutine[Any, Any, Any]:
        if self.instance is None:
            return self.decorator.asyncio(*args, **kwargs)
        else:
            return self.decorator.asyncio(self.instance, *args, **kwargs)


class AsyncDecorator(PureAsyncDecorator):
    binder_cls = AsyncDecoratorBinder

    def __init__(self, fn, cls, kwargs={}, asyncio_fn=None):
        super().__init__(fn, cls, kwargs, asyncio_fn)

    def is_pure_async_fn(self):
        return False

    def asynq(self, *args, **kwargs):
        return self._call_pure(args, kwargs)

    def name(self):
        return "@asynq()"

    def __call__(self, *args, **kwargs):
        if is_asyncio_mode():
            raise RuntimeError("asyncio mode does not support synchronous calls")

        return self._call_pure(args, kwargs).value()


class AsyncAndSyncPairDecoratorBinder(AsyncDecoratorBinder):
    def __call__(self, *args, **kwargs):
        # the base class implementation adds .instance here, but we don't want that because we
        # called __get__ on the sync_fn manually; if we don't do this we'll end up adding self or
        # cls twice
        return self.decorator(*args, **kwargs)


class AsyncAndSyncPairDecorator(AsyncDecorator):
    binder_cls = AsyncAndSyncPairDecoratorBinder

    def __init__(self, fn, cls, sync_fn, kwargs={}, asyncio_fn=None):
        AsyncDecorator.__init__(self, fn, cls, kwargs, asyncio_fn)
        self.sync_fn = sync_fn

    def __call__(self, *args, **kwargs):
        if is_asyncio_mode():
            raise RuntimeError("asyncio mode does not support synchronous calls")
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
            AsyncAndSyncPairDecorator,
            self.task_cls,
            sync_fn,
            self.kwargs,
            self.asyncio_fn,
        )(fn)
        return AsyncDecorator.__get__(new_self, owner, cls)


class AsyncProxyDecorator(AsyncDecorator):
    def __init__(self, fn, asyncio_fn=None):
        # we don't need the task class but still need to pass it to the superclass
        AsyncDecorator.__init__(self, fn, None, asyncio_fn=asyncio_fn)

    def asyncio(self, *args, **kwargs) -> Coroutine[Any, Any, Any]:
        if self.asyncio_fn is None:
            asyncio_fn = convert_asynq_to_async(self.fn)

            async def unwrap_coroutine(*args, **kwargs):
                return await (await asyncio_fn(*args, **kwargs))

            self.asyncio_fn = unwrap_coroutine

        return self.asyncio_fn(*args, **kwargs)

    def _call_pure(self, args, kwargs):
        if is_asyncio_mode():
            return self.asyncio(*args, **kwargs)

        return self.fn(*args, **kwargs)


class AsyncAndSyncPairProxyDecorator(AsyncProxyDecorator):
    def __init__(self, fn, sync_fn, asyncio_fn=None):
        AsyncProxyDecorator.__init__(self, fn, asyncio_fn=asyncio_fn)
        self.sync_fn = sync_fn

    def __call__(self, *args, **kwargs):
        return self.sync_fn(*args, **kwargs)


def asynq(
    pure=False, sync_fn=None, cls=async_task.AsyncTask, asyncio_fn=None, **kwargs
):
    """Async task decorator.
    Converts a method returning generator object to
    a method returning AsyncTask object.

    """
    if kwargs:
        assert pure, "custom kwargs are only supported with pure=True"
    if pure:
        assert sync_fn is None, "sync_fn is not supported for pure async functions"

    def decorate(fn):
        assert not (
            is_pure_async_fn(fn) or has_async_fn(fn)
        ), "@asynq() decorator can be applied just once"
        if pure:
            return qcore.decorators.decorate(PureAsyncDecorator, cls, kwargs)(fn)
        elif sync_fn is None:
            decorated = qcore.decorators.decorate(
                AsyncDecorator, cls, kwargs, asyncio_fn
            )(fn)
            return decorated
        else:
            return qcore.decorators.decorate(
                AsyncAndSyncPairDecorator, cls, sync_fn, kwargs, asyncio_fn
            )(fn)

    return decorate


def async_proxy(pure=False, sync_fn=None, asyncio_fn=None):
    if sync_fn is not None:
        assert pure is False, "sync_fn=? cannot be used together with pure=True"

    def decorate(fn):
        if pure:
            return fn
        if sync_fn is None:
            return qcore.decorators.decorate(AsyncProxyDecorator, asyncio_fn)(fn)
        else:
            return qcore.decorators.decorate(
                AsyncAndSyncPairProxyDecorator, sync_fn, asyncio_fn
            )(fn)

    return decorate


async def asyncio_call(fn, *args, **kwargs):
    """An asyncio-version of async_call.

    Note that it is not always possible to detect whether an function is a coroutine function or not.
    For example, a callable F that returns Coroutine is a coroutine function, but inspect.iscoroutinefunction(F) is False.

    """
    if is_pure_async_fn(fn):
        return await fn.asyncio(*args, **kwargs)
    elif hasattr(fn, "asynq"):
        return await fn.asyncio(*args, **kwargs)
    elif hasattr(fn, "async"):
        return await getattr(fn, "async")(*args, **kwargs)
    else:
        return fn(*args, **kwargs)


@async_proxy(asyncio_fn=asyncio_call)
def async_call(fn, *args, **kwargs):
    """Use this if you are not sure if fn is async or not.

    e.g. when you are within an async function and you need to call fn but it could either be
    async or non-async, you should write
    val = yield async_call.asynq(fn, arg1, kw1=value1)

    """
    if is_pure_async_fn(fn):
        return fn(*args, **kwargs)
    elif hasattr(fn, "asynq"):
        return fn.asynq(*args, **kwargs)
    elif hasattr(fn, "async"):
        return getattr(fn, "async")(*args, **kwargs)
    else:
        return futures.ConstFuture(fn(*args, **kwargs))


class AsyncWrapper(qcore.decorators.DecoratorBase):
    """Implements make_async_decorator."""

    binder_cls = AsyncDecoratorBinder

    def __init__(self, fn, wrapper_fn, name):
        super(AsyncWrapper, self).__init__(fn)
        self.wrapper_fn = wrapper_fn
        self.function_name = name

    def name(self):
        return "@%s()" % self.function_name

    def _call_async(self, args, kwargs):
        return self.wrapper_fn(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self._call_async(args, kwargs).value()

    def asynq(self, *args, **kwargs):
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
    return qcore.decorators.decorate(AsyncWrapper, wrapper_fn, name)(fn)
