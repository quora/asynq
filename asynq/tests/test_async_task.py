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

from asynq import async
from asynq.futures import ConstFuture

from qcore.asserts import assert_eq, assert_is, AssertRaises


def check_unwrap(expected, to_unwrap):
    # can't call unwrap() directly because it's hidden by cython
    @async()
    def caller():
        value = yield to_unwrap
        assert_eq(expected, value, extra='yielded {}'.format(to_unwrap))
    caller()


def test_unwrap():
    check_unwrap(None, None)
    check_unwrap(1, ConstFuture(1))

    for typ in (list, tuple):
        check_unwrap(typ(), typ())
        check_unwrap(typ([1]), typ([ConstFuture(1)]))
        check_unwrap(typ([1, 2]), typ([ConstFuture(1), ConstFuture(2)]))
        check_unwrap(typ([1, 2, 3]), typ([ConstFuture(1), ConstFuture(2), ConstFuture(3)]))

    with AssertRaises(TypeError):
        check_unwrap(1)
