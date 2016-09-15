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

from asynq import async, result
from .helpers import Profiler

counter = 0


def test():
    global counter

    @async(pure=True)
    def incr():
        global counter
        counter += 1
        print("Counter: %i" % counter)
        result(counter); return
        yield

    def sync_incr():
        global counter
        counter += 1
        print("Counter: %i" % counter)
        return counter

    @async(pure=True)
    def test_async():
        global counter
        try:
            print('In try block.')
            yield incr()
            result((yield incr()))  # ; return
            assert False, "Must not reach this point!"
        except BaseException as e:
            print('In except block, e = ' + repr(e))
            assert sync_incr() == 3
            if isinstance(e, GeneratorExit):
                raise
            assert False, "Must not reach this point!"
        finally:
            print('In finally block.')
            assert sync_incr() == 4

    with Profiler('test_stop()'):
        r = test_async()()
        assert r == 2
        assert counter == 4
    print()
