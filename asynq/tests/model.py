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

import qcore
from qcore.asserts import assert_eq
from asynq import async, result, cached
from .debug_cache import db, mc, miss, reset_cache_batch_indexes, flush_and_clear_local_caches
from .helpers import Profiler
from .entities import Entity, EntityField, PersisterBase


@cached(mc)
@async(pure=True)
def get_user(id):
    user = yield db.get('User: ' + id)
    if user is miss:
        result(None); return
    else:
        user['id'] = id
        result(user); return


@cached(mc)
@async(pure=True)
def get_passport(id):
    passport = yield db.get('Passport: ' + str(id))
    if passport is miss:
        result(None); return
    else:
        passport['id'] = id
        result(passport); return


@cached(mc)
@async(pure=True)
def get_user_passport(user_id):
    user = yield get_user(user_id)
    if user is None:
        result(None); return
    else:
        passport = yield get_passport(user['passport_id'])
        result((user, passport)); return


@async(pure=True)
def set_user(id, user):
    if 'id' in user:
        del user['id']
    get_user.dirty(id)
    get_user_passport.dirty(id)
    yield db.set('User: ' + id, user)


@async(pure=True)
def set_passport(id, passport):
    if 'id' in passport:
        del passport['id']
    get_passport.dirty(id)
    if passport is not None:
        get_user_passport.dirty(id, passport['user_id'])
    yield db.set('Passport: ' + str(id), passport)


class SimplePersister(PersisterBase):
    def __init__(self, get_method, set_method):
        self.get_method = get_method
        self.set_method = set_method

    @async(pure=True)
    def create(self, entity):
        yield self.update(entity, True)

    @async(pure=True)
    def load(self, entity):
        key = self.get_key(entity)
        data = yield self.get_method(key)
        if data is None:
            raise KeyError(
                "%s with key=%r does not exist." %
                (qcore.inspection.get_full_name(type(entity)), key))
        entity.update_data(data, mark_changed=False)

    @async(pure=True)
    def update(self, entity, create=False):
        print('update %r' % entity)
        key = self.get_key(entity)
        data = yield self.get_method(key)
        if create:
            if data is not None:
                raise KeyError(
                    "%s with key=%r already exists." %
                    (qcore.inspection.get_full_name(type(entity)), key))
            data = {}
        else:
            if data is None:
                raise KeyError(
                    "%s with key=%r does not exist." %
                    (qcore.inspection.get_full_name(type(entity)), key))
        data.update(entity.get_data(changes_only=True))
        yield self.set_method(key, data)

    @async(pure=True)
    def delete(self, entity):
        key = self.get_key(entity)
        data = yield self.get_method(key)
        if data is None:
            raise KeyError(
                "%s with id=%r does not exist." %
                (qcore.inspection.get_full_name(type(entity)), key))
        yield self.set_method(key, data)

    def get_key(self, entity):
        key = entity.key
        if len(key) == 1:
            key = key[0]
        return str(key)


class User(Entity):
    persister = SimplePersister(get_user, set_user)
    id = EntityField().key()
    passport_id = EntityField()

    @property
    def passport(self):
        return Passport.get(self.passport_id)

    @passport.setter
    def passport(self, value):
        self.passport_id = None if value is None else value.id


class Passport(Entity):
    persister = SimplePersister(get_passport, set_passport)
    id = EntityField().key()
    name = EntityField()
    surname = EntityField()
    user_id = EntityField()

    @property
    def user(self):
        return User.get(self.user_id)

    @user.setter
    def user(self, value):
        self.user_id = None if value is None else value.id


# No-yield methods

@cached(mc)
def get_user_passport_no_yield(user_id):
    user = get_user(user_id)()
    if user is None:
        return None
    else:
        passport = get_passport(user['passport_id'])()
        return (user, passport)


# DB initialization

@async(pure=True)
def init():
    reset_cache_batch_indexes()
    with Profiler('init()'):
        js = User.new('JS')
        js_p = Passport.new(1)
        js_p.name = 'John'
        js_p.surname = 'Smith'
        js.passport = js_p
        js_p.user = js

        ay = User.new('AY')
        ay_p = Passport.new(2)
        ay_p.name = 'Alex'
        ay_p.surname = 'Yakunin'
        ay.passport = ay_p
        ay_p.user = ay

        print(js)
        print(js_p)

        yield js, ay, js_p, ay_p

        print(js)
        print(js_p)

        flush_and_clear_local_caches()
        assert_eq(9, mc._batch.index)
        assert_eq(9, db._batch.index)
    print()
