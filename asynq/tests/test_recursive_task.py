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

from asynq import asynq, result, debug
from qcore.asserts import AssertRaises


@asynq()
def bad_function():
    yield bad_function.asynq()


def test_it():
    # Adjust max stack size to make test take less memory and time
    default_stack_limit = debug.options.MAX_TASK_STACK_SIZE
    try:
        debug.options.MAX_TASK_STACK_SIZE = 200
        with AssertRaises(RuntimeError):
            bad_function()
    finally:
        debug.options.MAX_TASK_STACK_SIZE = default_stack_limit
