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

from asynq import await
from .debug_cache import reset_caches
from .helpers import Profiler
from .model import reset_cache_batch_indexes, get_user_passport_no_yield, mc, db, flush_and_clear_local_caches, init


def test():
    def test1():
        reset_cache_batch_indexes()
        with Profiler('test1()'):
            (js, jsp), (ay, ayp), none = await(
                get_user_passport_no_yield('JS'),
                get_user_passport_no_yield('AY'),
                get_user_passport_no_yield('none'))

            assert js['passport_id'] == 1
            assert ay['passport_id'] == 2
            assert jsp['name'] == 'John'
            assert ayp['name'] == 'Alex'
            assert none is None

            flush_and_clear_local_caches()
            assert mc._batch.index == 6  # +1 batch, and this is the best possible result here!
            assert db._batch.index == 4  # +1 batch, and this is the best possible result here!
        print()

    def test2():
        reset_cache_batch_indexes()
        with Profiler('test2()'):
            user_ids = ['AY', 'JS']
            for user, passport in await(tuple(map(get_user_passport_no_yield, user_ids))):
                assert user['passport_id'] != None
                assert passport['user_id'] != None

            flush_and_clear_local_caches()
            assert mc._batch.index == 2
            assert db._batch.index == 1
        print()

    with Profiler('test_batching_no_yield()'):
        reset_caches()
        init()()

        # Improve batching efficiency in this case
        mc.run_scheduler_before_flush = True
        try:
            test1()
            test2()
        finally:
            mc.run_scheduler_before_flush = False
    print()
