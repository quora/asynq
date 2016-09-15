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

from . import scheduler


class NonAsyncContext(object):
    """Indicates that context can't contain yield statements.

    It means while a NonAsyncContext is active, async tasks cannot
    yield the control back to scheduler.

    Contexts should subclass this class if they want it throw an AssertionError
    if async tasks end up yielding within them.

    Note: Remember to call super inside your __enter__/__exit__.

    """

    def __enter__(self):
        self._active_task = enter_context(self)

    def __exit__(self, typ, val, tb):
        leave_context(self, self._active_task)

    def __pause__(self):
        assert False, 'Task %s cannot yield while %s is active' % (self._active_task, self)

    def __resume__(self):
        assert False, 'Task %s cannot yield while %s is active' % (self._active_task, self)


def enter_context(context):
    active_task = scheduler.get_active_task()
    if active_task is not None:
        active_task._enter_context(context)
    return active_task


def leave_context(context, active_task):
    if active_task is not None:
        active_task._leave_context(context)


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
        self._active_task = enter_context(self)
        self.__resume__()
        return self

    def __exit__(self, ty, value, tb):
        leave_context(self, self._active_task)
        self.__pause__()

    def __resume__(self):
        self._is_active = True
        self.resume()

    def resume(self):
        raise NotImplementedError()

    def __pause__(self):
        self.pause()
        self._is_active = False

    def pause(self):
        raise NotImplementedError()
