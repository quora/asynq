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

from asynq import async, result, cached
from .debug_cache import db, miss, mc, reset_cache_batch_indexes, flush_and_clear_local_caches


@cached(mc)
@async(pure=True)
def get_user(user_id):
    user = yield db.get('User: ' + str(user_id))
    result(None if user is miss else user); return


@cached(mc)
@async(pure=True)
def get_question(question_id):
    passport = yield db.get('Passport: ' + str(id))
    result(None if passport is miss else passport); return


@cached(mc)
@async(pure=True)
def get_question_user(question_id):
    question = yield get_question(question_id)
    user = None
    if question is not None:
        user = yield get_user(question['user'])
    result((question, user)); return


@async(pure=True)
def set_user(user_id, user):
    old_user = yield get_user(user_id)
    if old_user is not None:
        for qid in old_user['questions']:
            get_question_user.dirty(qid)
    if user is not None:
        for qid in user['questions']:
            get_question_user.dirty(qid)
    get_user.dirty(user_id)
    yield db.set('User: %i' % user_id, user)


@async(pure=True)
def set_question(question_id, question):
    get_question.dirty(question_id)
    get_question_user.dirty(question_id)
    yield db.set('Question: %i' % question_id, question)


# DB initialization

@async(pure=True)
def init():
    reset_cache_batch_indexes()
    print('init():')
    yield (
        set_question(1, {'title': 'Who?', 'user': 2}),
        set_question(2, {'title': 'Where?', 'user': 2}),
        set_question(3, {'title': 'When?', 'user': 2}),
        set_user(1, {'name': 'John Smith', 'questions': []}),
        set_user(2, {'name': 'James Bond', 'questions': [1, 2, 3]}),
    )

    flush_and_clear_local_caches()
    reset_cache_batch_indexes()
    print()
