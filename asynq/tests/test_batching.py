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

from asynq import async, await
from .debug_cache import reset_caches
from .model import reset_cache_batch_indexes, get_user_passport, Profiler, flush_and_clear_local_caches, mc, db, init


def test():
    @async(pure=True)
    def test1():
        reset_cache_batch_indexes()
        with Profiler('test1()'):
            (js, jsp), (ay, ayp), none = yield (
                get_user_passport('JS'),
                get_user_passport('AY'),
                get_user_passport('none'))

            assert js['passport_id'] == 1
            assert ay['passport_id'] == 2
            assert jsp['name'] == 'John'
            assert ayp['name'] == 'Alex'
            assert none is None

            flush_and_clear_local_caches()
            assert mc._batch.index == 5
            assert db._batch.index == 3
        print()

    @async(pure=True)
    def test2():
        reset_cache_batch_indexes()
        with Profiler('test2()'):
            user_ids = ['AY', 'JS']
            for user, passport in (yield tuple(map(get_user_passport, user_ids))):
                assert user['passport_id'] != None
                assert passport['user_id'] != None

            flush_and_clear_local_caches()
            assert mc._batch.index == 2
            assert db._batch.index == 1
        print()

    with Profiler('test_batching()'):
        reset_caches()
        init()()
        test1()()
        test2().start()()  # Actually the same as simply test2()()
    print()

