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

from asynq import async, scheduler
from qcore.asserts import assert_is, assert_is_not


@async()
def outer_async():
    """Test that we get the correct active task from the scheduler.

    Previously, there was a bug such that when we called a task synchronously from an async
    function, the scheduler would lose track of the outer function after executing the inner
    function, causing this test to fail.

    """
    assert_is_not(None, scheduler.get_active_task())
    assert_is(outer_async.fn, scheduler.get_active_task().fn)
    inner_async()
    assert_is_not(None, scheduler.get_active_task())
    assert_is(outer_async.fn, scheduler.get_active_task().fn)


@async()
def inner_async():
    pass


def test_active_task():
    outer_async()
