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
from .debug_cache import reset_caches
from .sample_model import get_question, get_question_user, get_user, init


@async(pure=True)
def async_test():
    print('Test:')
    question_ids = [1, 2, 3, 100]
    user_ids = [1, 2, 100]

    print('Questions and users:')
    for question, user in (yield tuple(map(get_question_user, question_ids))):
        print(('Question: %s, User: %s' % (str(question), str(user))))
    print()

#    print 'Questions:'
#    for question in (yield tuple(map(get_question, question_ids))):
#        print 'Question: %s' % (str(question))
#    print
#
#    print 'Users:'
#    for user in (yield tuple(map(get_user, user_ids))):
#        print 'User: %s' % (str(user))
#    print


def test():
    reset_caches()
    init()()
    async_test()()
    await()
#    flush_and_clear_local_caches()
#    async_test()()
#    flush_and_clear_local_caches()


if __name__ == '__main__':
    test()
