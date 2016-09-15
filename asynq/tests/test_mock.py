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

import asynq
from qcore.asserts import assert_eq, assert_is, assert_not_in, AssertRaises

# ===================================================
# Objects to mock
# ===================================================


@asynq.async()
def fn():
    pass


@asynq.async()
def async_caller():
    ret = yield fn.async()
    asynq.result((ret, fn())); return


def non_async_caller():
    return fn()


class Cls(object):
    @asynq.async()
    @classmethod
    def async_classmethod(cls):
        pass

    def non_async_method(self, foo):
        pass

    @asynq.async()
    def async_method(self):
        return 'capybaras'

instance = Cls()


@asynq.async()
def class_method_async_caller():
    ret = yield Cls.async_classmethod.async()
    asynq.result((ret, Cls.async_classmethod())); return


def class_method_non_async_caller():
    return Cls.async_classmethod()


@asynq.async()
def method_async_caller():
    obj = Cls()
    ret = yield obj.async_method.async()
    asynq.result((ret, obj.async_method())); return


def method_non_async_caller():
    return Cls().async_method()


class Counter(object):
    linked_class = Cls

    def __init__(self):
        self.count = 0

    def add_call(self):
        self.count += 1


class SecondClass(object):
    pass


IMPORTANT_DICTIONARY = {'capybaras': 'guinea pigs', 'hutias': 'coypus'}

# ===================================================
# Helpers for testing that mocks work right.
# ===================================================


class MockChecker(object):
    @classmethod
    def check(cls, mock_fn, mock_classmethod, mock_method):
        cls._check_mock(mock_fn, async_caller, non_async_caller)
        cls._check_mock(mock_classmethod, class_method_async_caller, class_method_non_async_caller)
        cls._check_mock(mock_method, method_async_caller, method_non_async_caller)

    @classmethod
    def _check_mock(cls, mock_fn, async_caller, non_async_caller):
        raise NotImplementedError


class MockCheckerWithAssignment(MockChecker):
    @classmethod
    def _check_mock(cls, mock_fn, async_caller, non_async_caller):
        mock_fn.return_value = 42
        assert_eq(0, mock_fn.call_count)
        assert_eq(42, non_async_caller())
        assert_eq(1, mock_fn.call_count)

        assert_eq((42, 42), async_caller())
        assert_eq(3, mock_fn.call_count)

        mock_fn.side_effect = lambda: 43
        assert_eq(43, non_async_caller())
        assert_eq((43, 43), async_caller())


class MockCheckerWithNew(MockChecker):
    @classmethod
    def _check_mock(cls, mock_fn, async_caller, non_async_caller):
        assert_eq(42, non_async_caller())
        assert_eq((42, 42), async_caller())


# ===================================================
# Actual tests.
# ===================================================


def test_mock_async_context():
    with asynq.mock.patch('asynq.tests.test_mock.fn') as mock_fn, \
            asynq.mock.patch.object(Cls, 'async_classmethod') as mock_classmethod, \
            asynq.mock.patch.object(Cls, 'async_method') as mock_method:
        MockCheckerWithAssignment.check(mock_fn, mock_classmethod, mock_method)

    with asynq.mock.patch('asynq.tests.test_mock.fn', lambda: 42) as mock_fn, \
            asynq.mock.patch.object(Cls, 'async_classmethod', classmethod(lambda _: 42)) as mock_classmethod, \
            asynq.mock.patch.object(Cls, 'async_method', lambda _: 42) as mock_method:
        MockCheckerWithNew.check(mock_fn, mock_classmethod, mock_method)


@asynq.mock.patch('asynq.tests.test_mock.fn')
@asynq.mock.patch.object(Cls, 'async_classmethod')
@asynq.mock.patch('asynq.tests.test_mock.Cls.async_method')
def test_mock_async_decorator(mock_method, mock_classmethod, mock_fn):
    MockCheckerWithAssignment.check(mock_fn, mock_classmethod, mock_method)


@asynq.mock.patch('asynq.tests.test_mock.fn')
@asynq.mock.patch.object(Cls, 'async_classmethod')
@asynq.mock.patch('asynq.tests.test_mock.Cls.async_method')
class TestMockAsyncClassDecorator(object):
    def test(self, mock_method, mock_classmethod, mock_fn):
        MockCheckerWithAssignment.check(mock_fn, mock_classmethod, mock_method)


@asynq.mock.patch('asynq.tests.test_mock.fn', lambda: 42)
@asynq.mock.patch.object(Cls, 'async_classmethod', classmethod(lambda _: 42))
@asynq.mock.patch('asynq.tests.test_mock.Cls.async_method', lambda _: 42)
def test_mock_async_decorator_with_new():
    MockCheckerWithNew.check(fn, Cls.async_classmethod, Cls().async_method)


@asynq.mock.patch('asynq.tests.test_mock.fn', lambda: 42)
@asynq.mock.patch.object(Cls, 'async_classmethod', classmethod(lambda _: 42))
@asynq.mock.patch('asynq.tests.test_mock.Cls.async_method', lambda _: 42)
class TestMockAsyncClassDecoratorWithNew(object):
    def test(self):
        MockCheckerWithNew.check(fn, Cls.async_classmethod, Cls().async_method)


def test_mock_async_dict():
    assert_eq('guinea pigs', IMPORTANT_DICTIONARY['capybaras'])

    with asynq.mock.patch.dict(
            'asynq.tests.test_mock.IMPORTANT_DICTIONARY', {'capybaras': 'maras'}):
        assert_eq('maras', IMPORTANT_DICTIONARY['capybaras'])
        assert_eq('coypus', IMPORTANT_DICTIONARY['hutias'])
    assert_eq('guinea pigs', IMPORTANT_DICTIONARY['capybaras'])

    with asynq.mock.patch.dict('asynq.tests.test_mock.IMPORTANT_DICTIONARY', {'capybaras': 'maras'},
                               clear=True):
        assert_eq('maras', IMPORTANT_DICTIONARY['capybaras'])
        assert_not_in('hutias', IMPORTANT_DICTIONARY)


def test_maybe_wrap_new():
    counter = Counter()
    with asynq.mock.patch('asynq.tests.test_mock.fn', counter.add_call):
        fn()
    assert_eq(1, counter.count)

    with asynq.mock.patch('asynq.tests.test_mock.fn', 'capybara'):
        assert_eq('capybara', fn)



class TestPatchMethodWithMethod(object):
    def setup(self):
        self.calls = []

    def mock_method(self, foo):
        self.calls.append(foo)

    def test(self):
        with asynq.mock.patch.object(instance, 'non_async_method', self.mock_method):
            assert_eq([], self.calls)
            instance.non_async_method('bar')
            assert_eq(['bar'], self.calls)


def test_patch_class():
    with asynq.mock.patch.object(Counter, 'linked_class', SecondClass):
        assert_is(SecondClass, Counter.linked_class)


def test_cant_set_attribute():
    with asynq.mock.patch('asynq.tests.test_mock.fn'):
        with AssertRaises(TypeError):
            fn.async.cant_set_attribute = 'capybara'
