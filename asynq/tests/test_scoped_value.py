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

from qcore.asserts import assert_eq
from asynq import AsyncScopedValue, async, result, async_override
from asynq.batching import DebugBatchItem

v = AsyncScopedValue('a')


@async()
def async_scoped_value_helper(inner_val):
    @async()
    def nested():
        assert_eq(v.get(), inner_val)
        yield DebugBatchItem()
        with v.override('c'):
            yield DebugBatchItem()  # just so other function gets scheduled
            assert_eq(v.get(), 'c')
            yield DebugBatchItem()

    assert_eq(v.get(), 'a')
    yield DebugBatchItem()
    with v.override(inner_val):
        yield DebugBatchItem()
        assert_eq(v.get(), inner_val)
        result((yield nested.async())); return


@async()
def async_scoped_value_caller():
    yield async_scoped_value_helper.async('e'), async_scoped_value_helper.async('f')


def test_async_scoped_value():

    async_scoped_value_caller()

    val = AsyncScopedValue('capybara')
    assert_eq('capybara', val.get())
    val.set('nutria')
    assert_eq('nutria', val.get())

    assert_eq('AsyncScopedValue(nutria)', str(val))
    assert_eq("AsyncScopedValue('nutria')", repr(val))


def test_exception():
    @async()
    def test_body():
        assert_eq(v(), 'a')
        yield
        try:
            with v.override('b'):
                yield
                assert_eq(v(), 'b')
                yield
                raise NotImplementedError()
        except NotImplementedError:
            yield
            pass
        yield
        assert_eq(v(), 'a')

    test_body()


def test_override():
    class TestObject(object):
        def __init__(self):
            self.v = None

    o = TestObject()
    o.v = 'a'

    @async()
    def test_body():
        assert_eq(o.v, 'a')
        yield
        with async_override(o, 'v', 'b'):
            assert_eq(o.v, 'b')
            yield
            try:
                with async_override(o, 'v', 'c'):
                    assert_eq(o.v, 'c')
                    yield
                    raise NotImplementedError()
            except NotImplementedError:
                pass
            assert_eq(o.v, 'b')
        yield
        assert_eq(o.v, 'a')

    test_body()
