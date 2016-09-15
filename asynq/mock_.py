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

Easy mocking for async functions.

This module provides a patch() function that is based on the standard library's mock.patch but
supports mocking async functions.

This file is named mock_ instead of mock so that it can import the standard mock module.

"""

import inspect

try:
    import mock
except ImportError:
    # py3
    from unittest import mock

try:
    _patch = mock._patch
except AttributeError:
    # some versions of mock put the implementation in mock/mock.py
    _patch = mock.mock._patch
try:
    _get_target = mock._get_target
except AttributeError:
    # some versions of mock put the implementation in mock/mock.py
    _get_target = mock.mock._get_target


from .decorators import async
from .futures import ConstFuture


def patch(
        target, new=mock.DEFAULT, spec=None, create=False,
        mocksignature=False, spec_set=None, autospec=False,
        new_callable=None, **kwargs
):
    """Mocks an async function.

    Should be a drop-in replacement for mock.patch that handles async automatically. The .async
    attribute is automatically created and shouldn't be used when accessing data on
    the mock.

    """
    getter, attribute = _get_target(target)
    return _make_patch_async(
        getter, attribute, new, spec, create, mocksignature, spec_set, autospec, new_callable,
        kwargs
    )


def _patch_object(
        target, attribute, new=mock.DEFAULT, spec=None,
        create=False, mocksignature=False, spec_set=None, autospec=False,
        new_callable=None, **kwargs
):
    getter = lambda: target
    return _make_patch_async(
        getter, attribute, new, spec, create, mocksignature, spec_set, autospec, new_callable,
        kwargs
    )


patch.object = _patch_object
# duplicate mock.patch.dict for compatibility
patch.dict = mock.patch.dict
patch.TEST_PREFIX = mock.patch.TEST_PREFIX


def _make_patch_async(getter, attribute, new, spec, create, mocksignature,
                      spec_set, autospec, new_callable, kwargs):
    new = _maybe_wrap_new(new)

    try:
        patch = _PatchAsync(
            getter, attribute, new, spec, create, mocksignature,
            spec_set, autospec, new_callable, kwargs
        )
    except TypeError:
        # Python 3 doesn't have mocksignature
        patch = _PatchAsync(
            getter, attribute, new, spec, create, spec_set, autospec, new_callable, kwargs
        )
    return patch


class _PatchAsync(_patch):
    def __enter__(self):
        mock_fn = super(_PatchAsync, self).__enter__()
        # so we can also mock non-functions for compatibility
        if callable(mock_fn):
            async_fn = _AsyncWrapper(mock_fn)
            mock_fn.async = async_fn
        return mock_fn

    start = __enter__

    def copy(self):
        """Identical to the superclass except that a _PatchAsync object is created."""
        if hasattr(self, 'mocksignature'):  # Python 2
            patcher = _PatchAsync(
                self.getter, self.attribute, self.new, self.spec,
                self.create, self.mocksignature, self.spec_set,
                self.autospec, self.new_callable, self.kwargs
            )
        else:
            patcher = _PatchAsync(
                self.getter, self.attribute, self.new, self.spec,
                self.create, self.spec_set, self.autospec, self.new_callable, self.kwargs
            )
        patcher.attribute_name = self.attribute_name
        patcher.additional_patchers = [
            p.copy() for p in self.additional_patchers
        ]
        return patcher


class _AsyncWrapper(object):
    """Wrapper for the .async attribute of patch'ed functions.

    Prevents people from setting and reading attributes on them.

    """

    def __init__(self, mock_fn):
        object.__setattr__(self, '_mock_fn', mock_fn)

    def __call__(self, *args, **kwargs):
        return ConstFuture(self._mock_fn(*args, **kwargs))

    def __setattr__(self, attr, value):
        raise TypeError(
            'You cannot set attributes directly on a .async function. Set them on the '
            'function itself instead.'
        )

    def __getattr__(self, attr):
        raise TypeError(
            'You cannot read attributes directly on a .async function. Read them on the '
            'function itself instead.'
        )


def _maybe_wrap_new(new):
    """If the mock replacement cannot have attributes set on it, wraps it in a function.

    Also, if the replacement object is a method, applies the async() decorator.

    This is needed so that we support patch(..., x.method) where x.method is an instancemethod
    object, because instancemethods do not support attribute assignment.

    """
    if new is mock.DEFAULT:
        return new

    if inspect.isfunction(new) or isinstance(new, (classmethod, staticmethod)):
        return async(sync_fn=new)(new)
    elif not callable(new):
        return new

    try:
        new._maybe_wrap_new_test_attribute = None
        del new._maybe_wrap_new_test_attribute
    except (AttributeError, TypeError):
        # setting something on a bound method raises AttributeError, setting something on a
        # Cythonized class raises TypeError
        should_wrap = True
    else:
        should_wrap = False

    if should_wrap:
        # we can't just use a lambda because that overrides __get__ and creates bound methods we
        # don't want, so we make a wrapper class that overrides __call__
        class Wrapper(object):
            def __call__(self, *args, **kwargs):
                return new(*args, **kwargs)
        return Wrapper()
    else:
        return new
