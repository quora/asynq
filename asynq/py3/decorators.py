import qcore.decorators
import qcore

from .async_task import AsyncTask
from . import batching
from asynq.decorators import is_pure_async_fn, is_async_fn


class PureAsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    def is_pure_async_fn(self):
        return True


class PureAsyncDecorator(qcore.decorators.DecoratorBase):
    binder_cls = PureAsyncDecoratorBinder

    def name(self):
        return '@async3(pure=True)'

    def is_pure_async_fn(self):
        return True

    def __call__(self, *args, **kwargs):
        return AsyncTask(self.fn, args, kwargs)


class AsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    def future(self, *args, **kwargs):
        if self.instance is None:
            return self.decorator.future(*args, **kwargs)
        else:
            return self.decorator.future(self.instance, *args, **kwargs)

    async = future


class AsyncDecorator(PureAsyncDecorator):
    binder_cls = AsyncDecoratorBinder

    def is_pure_async_fn(self):
        return False

    def future(self, *args, **kwargs):
        return AsyncTask(self.fn, args, kwargs)

    async = future

    def name(self):
        return '@async3()'

    def __call__(self, *args, **kwargs):
        return AsyncTask(self.fn, args, kwargs).value()


class AsyncProxyDecorator(AsyncDecorator):

    def future(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    async = future

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs).value()


def coroutine(pure=False):
    """Async task decorator.

    To be applied to generator functions that depend on other coroutines.
    Allows you to either evaluate it or get a Task (future) object out ot it.

    """

    def decorate(fn):
        if pure:
            return qcore.decorators.decorate(PureAsyncDecorator)(fn)
        else:
            return qcore.decorators.decorate(AsyncDecorator)(fn)

    return decorate


async = coroutine


def async_proxy():
    def decorate(fn):
        return qcore.decorators.decorate(AsyncProxyDecorator)(fn)
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
    return batching.ConstFuture(fn(*args, **kwargs))


def get_async_fn(fn, wrap_if_none=False):
    """Returns an async function for the specified source function."""
    if hasattr(fn, 'async'):
        return fn.async
    if is_pure_async_fn(fn):
        return fn
    if wrap_if_none:
        def sync_to_async_fn_wrapper(*args, **kwargs):
            return batching.ConstFuture(fn(*args, **kwargs))

        sync_to_async_fn_wrapper.is_pure_async_fn = qcore.helpers.true_fn
        return sync_to_async_fn_wrapper
    return None
