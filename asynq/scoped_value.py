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

Helpers to use scoped values within async functions.

Similar to AsyncContext in that the scope "pauses" and "resumes"
as the scheduler pauses and resumes the execution of the function
the scoped values are defined/overridden within.

See tests/test_scoped_value.py for examples.

"""

import qcore

from . import contexts


_empty_context = qcore.empty_context


class AsyncScopedValue(object):
    def __init__(self, default):
        self._value = default

    def get(self):
        return self._value

    def set(self, value):
        self._value = value

    def override(self, value):
        """Temporarily overrides the old value with the new one."""
        return _AsyncScopedValueOverrideContext(self, value)

    def __call__(self):
        """Same as get."""
        return self._value

    def __str__(self):
        return 'AsyncScopedValue(%s)' % str(self._value)

    def __repr__(self):
        return 'AsyncScopedValue(%s)' % repr(self._value)


class _AsyncScopedValueOverrideContext(contexts.AsyncContext):
    def __init__(self, target, value):
        self._target = target
        self._value = value
        self._old_value = None

    def resume(self):
        self._old_value = self._target._value
        self._target._value = self._value

    def pause(self):
        self._target._value = self._old_value

    def __repr__(self):
        return '_AsyncScopedValueOverrideContext(target=%r, value=%r)' % (self._target, self._value)


class _AsyncPropertyOverrideContext(contexts.AsyncContext):
    def __init__(self, target, property_name, value):
        self._target = target
        self._property_name = property_name
        self._value = value
        self._old_value = None

    def resume(self):
        self._old_value = getattr(self._target, self._property_name)
        setattr(self._target, self._property_name, self._value)

    def pause(self):
        setattr(self._target, self._property_name, self._old_value)

    def __repr__(self):
        return '_AsyncPropertyOverrideContext(target=%r, property_name=%r, value=%r)' % (
            self._target,
            self._property_name,
            self._value
        )

async_override = _AsyncPropertyOverrideContext
globals()['async_override'] = async_override
