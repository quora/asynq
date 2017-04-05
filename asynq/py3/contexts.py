import async_task


class AsyncContext(object):
    """Base class for contexts that should pause and resume during an async's function execution.

    Your context should subclass this class and implement pause and resume (at least).

    That would make the context pause and resume each time the execution of the async function
    within this context is paused and resumed.

    Additionally, you can also subclass __enter__ and __exit__ if you want to customize its
    behaviour. Remember to call super in that case.

    NOTE: __enter__/__exit__ methods automatically call resume/pause so the overridden
    __enter__/__exit__ methods shouldn't do that explicitly.

    """

    def __enter__(self):
        self._active_task = async_task.register_context(self)
        self.resume()
        return self

    def __exit__(self, ty, value, tb):
        async_task.leave_context(self, self._active_task)
        self.pause()

    def resume(self):
        raise NotImplementedError()

    def pause(self):
        raise NotImplementedError()
