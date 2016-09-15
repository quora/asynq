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

from qcore.asserts import AssertRaises, assert_eq
try:
    import cPickle as pickle
except ImportError:
    import pickle

from asynq import async
from .entities import unknown
from .helpers import Profiler
from .debug_cache import reset_caches
from . import model


def test():
    @async(pure=True)
    def async_test():
        model.reset_cache_batch_indexes()
        with Profiler('async_test()'):
            js, ay = yield model.User.get('JS'), model.User.get('AY')
            assert 1 == js.passport.id
            assert 2 == ay.passport.id

            jsp, ayp = yield js.passport, ay.passport

            assert jsp.name == 'John'
            assert ayp.name == 'Alex'

            # Changing user passports
            jsp.user = ay
            ayp.user = js
            js.passport = ayp
            ay.passport = jsp
            yield js, ay, jsp, ayp

            # Checking the result
            jsp, ayp = yield js.passport, ay.passport
            assert jsp.name == 'Alex'
            assert ayp.name == 'John'

            model.flush_and_clear_local_caches()
            assert_eq(8, model.mc._batch.index)
            assert_eq(11, model.db._batch.index)
        print()

    with Profiler('test_async_property()'):
        reset_caches()
        model.init()()
        async_test()()
    print()


def bad_cases_test():
    @async(pure=True)
    def async_test():
        with AssertRaises(AssertionError):
            model.User.new()
        with AssertRaises(AssertionError):
            model.User.new('AY', 'no matching field')
        with AssertRaises(AssertionError):
            model.User.new(None)
        with AssertRaises(AssertionError):
            model.User.new(unknown)

        ay = model.User.get('AY')
        with AssertRaises(AssertionError):
            ayp = yield ay.passport  # Key is unknown

        yield ay
        model.ayp = yield ay.passport

    reset_caches()
    model.init()()
    async_test()()


def pickle_test():
    # TODO: Fix this test
    return "Does not work w/Cython, TBD"

    @async(pure=True)
    def async_test():
        ay = yield model.User.get('AY')
        ayp = yield ay.passport
        ayp.name = 'Alexey'

        print('Before pickling:')
        print(ay, '\n', ayp)
        assert ayp.state.is_changed()

        ay, ayp = pickle_unpickle(ay, ayp)
        print('\nAfter pickling:')
        print(ay, '\n', ayp)
        assert ayp.state.is_changed()

        yield ay, ayp
        print('\nAfter sync:')
        print(ay, '\n', ayp)
        assert not ayp.state.is_changed()

    reset_caches()
    model.init()()
    async_test()()


def pickle_unpickle(*source):
    dump = pickle.dumps(source)
    return pickle.loads(dump)
