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

__doc__ = """

Tests that when using multiple inheritance, both parent classes' methods can be called.

See the implementation of DecoratorBase.__get__ for how this works.

"""

from asynq import async, result


called = {}


class Parent1(object):
    @async()
    def method(self):
        called['Parent1'] = True


class Parent2(object):
    @async()
    def method(self):
        called['Parent2'] = True


class Child(Parent1, Parent2):
    @async()
    def method(self):
        yield super(Child, self).method.async()
        yield Parent2.method.async(self)
        called['Child'] = True


def test():
    @async()
    def inner():
        instance = Child()
        yield instance.method.async()
        result(None); return

    inner()
    assert called['Parent1']
    assert called['Parent2']
    assert called['Child']
