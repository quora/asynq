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

from asynq import asynq, scheduler
from qcore.asserts import assert_is


@asynq()
def outer_async():
    """Test that we get the correct active task from the scheduler.

    Even when the execution of one task gets interrupted by a synchronous call to another async
    function, the scheduler retains the correct active task.

    """
    active_task = scheduler.get_active_task()
    inner_async()
    assert_is(active_task, scheduler.get_active_task())


@asynq()
def inner_async():
    pass


def test_active_task():
    outer_async()
