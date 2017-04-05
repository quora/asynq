import asyncio
import types
import threading

import batching


class AsyncTask:
    def __init__(self, fn, args, kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self._contexts = []

    def __str__(self):
        return 'AsyncTask(fn=%s, args=%s, kwargs=%s)' % (
            self.fn.__qualname__, self.args, self.kwargs)

    def value(self):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.future())

    def add_context(self, ctx):
        self._contexts.append(ctx)

    def leave_context(self, ctx):
        assert self._contexts[-1] == ctx
        self._contexts.pop(-1)

    def _before_continue(self):
        _state.current_task = self
        for ctx in self._contexts:
            ctx.resume()

    def _after_continue(self):
        for ctx in reversed(self._contexts):
            ctx.pause()
        _state.current_task = None

    def _extract_futures(self, result):
        if isinstance(result, AsyncTask):
            result._contexts = self._contexts[:]
            return result.future()
        if isinstance(result, batching.BatchItem):
            return result.future()
        if isinstance(result, (list, tuple)):
            futures = [self._extract_futures(elem) for elem in result]
            return asyncio.gather(*futures)
        assert False, 'result of type {!r} is not allowed'.format(type(result))

    async def future(self):
        # first get the generator.
        # need to do before/after continue in case the function is not a generator.
        self._before_continue()
        gen = self.fn(*self.args, **self.kwargs)
        self._after_continue()

        # if fn is not a generator, then we already have the value.
        if not isinstance(gen, types.GeneratorType):
            return gen

        send_value = None
        while True:
            try:
                self._before_continue()
                yield_result = gen.send(send_value)
            except StopIteration as e:
                return e.value
            finally:
                self._after_continue()
            send_value = await self._extract_futures(yield_result)
        assert False


def register_context(ctx):
    active_task = _state.current_task
    active_task.add_context(ctx)
    return active_task


def leave_context(ctx, task):
    assert (task == _state.current_task), '%r != %r' % (str(task), str(_state.current_task))
    task.leave_context(ctx)


class LocalState(threading.local):
    def __init__(self):
        super().__init__()
        self.current_task = None


_state = LocalState()
