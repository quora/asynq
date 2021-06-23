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

from asynq import asynq, AsyncContext
from qcore.asserts import AssertRaises, assert_eq

from .helpers import Profiler

counter = 0


def test():
    global counter

    @asynq(pure=True)
    def throw(expected_counter, must_throw):
        global counter
        print(
            "  In throw, counter=%i (expected %i), must_throw=%s"
            % (counter, expected_counter, str(must_throw))
        )
        assert expected_counter == counter
        if must_throw:
            raise RuntimeError
        counter += 1
        return counter

    @asynq(pure=True)
    def test():
        global counter
        counter = 0
        tasks = (throw(0, False), throw(1, True), throw(1, False))
        try:
            yield tasks
            raise AssertionError()
        except Exception:
            pass
        assert counter == 2
        assert tasks[0].value() == 1
        assert tasks[1].error() is not None
        assert tasks[2].value() == 2

    with Profiler("test_exceptions()"):
        test()()


context_is_active = 0


def test_async_context():
    class ContextThatRaises(AsyncContext):
        def __init__(self, raise_in_pause, raise_in_resume):
            self.raise_in_pause = raise_in_pause
            self.raise_in_resume = raise_in_resume

        def resume(self):
            if self.raise_in_resume:
                # we raise a BaseException because cythonized async
                # can ignore system generated exceptions as well.
                # e.g. we raise KeyboardInterrupt before restarting a webserver
                raise KeyboardInterrupt()

        def pause(self):
            if self.raise_in_pause:
                raise KeyboardInterrupt()

    class SimpleContext(AsyncContext):
        def resume(self):
            global context_is_active
            context_is_active += 1

        def pause(self):
            global context_is_active
            context_is_active -= 1

    @asynq()
    def dependency():
        return 1

    @asynq()
    def throw(raise_in_pause, raise_in_resume):
        with SimpleContext():
            with ContextThatRaises(raise_in_pause, raise_in_resume):
                with SimpleContext():
                    # we need this to have a real dependency on an async task, otherwise
                    # it executes the whole function inline and the real problem is never tested
                    val = yield dependency.asynq()
        return val

    def check_contexts_released_properly(raise_in_pause, raise_in_resume):
        with AssertRaises(KeyboardInterrupt):
            throw(raise_in_pause, raise_in_resume)

        global context_is_active
        assert_eq(0, context_is_active)

    check_contexts_released_properly(True, False)
    check_contexts_released_properly(False, True)
    check_contexts_released_properly(True, True)
