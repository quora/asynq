import qcore.decorators

from async_task import AsyncTask


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


class AsyncDecorator(PureAsyncDecorator):
    binder_cls = AsyncDecoratorBinder

    def is_pure_async_fn(self):
        return False

    def future(self, *args, **kwargs):
        return AsyncTask(self.fn, args, kwargs)

    def name(self):
        return '@async3()'

    def __call__(self, *args, **kwargs):
        return AsyncTask(self.fn, args, kwargs).value()


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
