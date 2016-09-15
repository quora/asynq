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

from qcore.asserts import AssertRaises
from six.moves import xrange

from asynq import async, debug, result
from .helpers import Profiler


def test_recursion():
    def sum(i, dump_progress=False):
        if i == 0:
            return 0
        if dump_progress and i % 100 == 0:
            print('... in sum(%s)' % i)
        return i + sum(i - 1, dump_progress)

    @async()
    def sum_async(i, dump_progress=False):
        if i == 0:
            result(0); return
        if dump_progress and i % 100 == 0:
            print('... in sum_async(%s)' % i)
        result(i + (yield sum_async.async(i - 1, dump_progress))); return

    with AssertRaises(RuntimeError):
        sum(2000, True)  # By default max stack depth is ~ 1000

    sum_async(2000, True)

    # But async is costly:

    p1 = Profiler("200 x sum(500)")
    with p1:
        for i in xrange(0, 200):
            sum(500)

    p2 = Profiler("20 x sum_async(500)() (w/assertions)")
    with p2:
        for i in xrange(0, 20):
            sum_async(500)

    print('Slowdown: %s' % (p2.diff * 10.0 / p1.diff))

    p3 = Profiler("20 x sum_async(500)() (w/o assertions)")
    with debug.disable_complex_assertions(), p3:
        for i in xrange(0, 20):
            sum_async(500)

    print('Slowdown: %s' % (p3.diff * 10.0 / p1.diff))

    p3 = Profiler("sum_async(10000)()")
    with p3:
        sum_async(10000)

