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

import qcore.helpers as core_helpers
import qcore.events as core_events
import qcore.errors as core_errors

from . import debug
from . import _debug


__traceback_hide__ = True


_debug_options = _debug.options
_none = core_helpers.MarkerObject(u"none (futures)")
globals()['_none'] = _none


class FutureIsAlreadyComputed(Exception):
    pass


class FutureBase(object):
    """Abstract base class for delayed computations (AKA
    futures or promises).

    Intended usage:

    * ``future.value()`` gets the value of future computation.
      Computes, if it's not cached yet.
    * ``future()`` does the same (just a shorter notation)

    """
    def __init__(self):
        self._value = _none
        self._error = None
        self._in_repr = False
        self.on_computed = core_events.EventHook()

    def value(self):
        """Gets the value of future.

        Computes, if necessary. If computation was completed
        with exception, it will be raised by this method.

        """
        if self._value is _none:
            self._compute()
        self.raise_if_error()
        return self._value

    def set_value(self, value):
        """Sets cached value and raises on_computed event.
        In addition, sets error property to None.

        """
        if self.is_computed():
            raise FutureIsAlreadyComputed(self)
        self._error = None
        self._value = value
        self._computed()

    def reset_unsafe(self):
        """Resets the cached value making it possible to compute
        the future once more time.

        Normally you should never use this method.

        """
        self._error = None
        self._value = _none

    def error(self):
        """Gets an error that was raised during value computation.

        If computation isn't executed yet, it will be executed by this method.
        Returns None, if there was no error.

        If error is provided, this method calls set_error
        passing provided value to it.

        """
        if self._value is _none:
            self._compute()
        return self._error

    def set_error(self, error):
        """Sets cached error and raises on_computed event.
        In addition, sets value property to None.
        """
        if self.is_computed():
            raise FutureIsAlreadyComputed(self)
        self._error = error
        self._value = None
        self._computed()

    def is_computed(self):
        """Returns true if value and error property values
        were already computed.

        """
        return self._value is not _none

    def _computed(self):
        """Protected method invoked when value or error property
        is set.

        Raises on_computed event.

        """
        if _debug_options.DUMP_COMPUTED:
            debug.write('@async: computed %s' % debug.str(self))
        # Must be a safe call, since we want any subscriber to get
        # the notification even in case one of them fails.
        # Otherwise lots of negative effects are possible - e.g.
        # some of them might be left in blocked state.
        self.on_computed.safe_trigger(self)

    def _compute(self):
        """Protected method invoked to acquire the value.
        Override it to implement value computation.
        It is required to set either value property, or error property
        (in any case both properties will be set).

        """
        raise NotImplementedError()

    def make_dependency(self, task, scheduler):
        """Makes this Future into a dependency for the given task.

        By default, does nothing. Subclasses can override this to implement dependency registration.

        """
        pass

    def raise_if_error(self):
        if self._error is not None:
            core_errors.reraise(self._error)

    def __call__(self):
        """The same as value property.
        Call arguments must be ignored.

        """
        return self.value()

    def __repr__(self):
        if self._in_repr:
            return '<recursion>'
        try:
            self._in_repr = True
            if self.is_computed():
                status = 'computed, '
                if self.error() is None:
                    if self.value() is self:
                        status += '= self'
                    else:
                        status += '= ' + repr(self.value())
                else:
                    status += 'error = ' + repr(self.error())
            else:
                status = "isn't computed"
            return '%s (%s)' % (type(self), status)
        finally:
            self._in_repr = False

    def dump(self, indent=0):
        debug.write(debug.str(self), indent)

    def __nonzero__(self):
        # treating a FutureBase object as a bool is probably a bug
        raise TypeError("Cannot convert FutureBase object to bool")


class Future(FutureBase):
    """Delegate-based future implementation."""
    def __init__(self, value_provider):
        super(Future, self).__init__()
        self._value_provider = value_provider

    def _compute(self):
        try:
            self.set_value(self._value_provider())
        except BaseException as error:
            self.set_error(error)
            raise


class ConstFuture(FutureBase):
    """Future wrapping the constant."""
    def __init__(self, value):
        self._value = _none
        self._error = None
        self.on_computed = core_events.sinking_event_hook  # Simple performance optimization
        self.set_value(value)
        self._in_repr = False  # since we don't call super.__init__

    def __getstate__(self):
        return self.value()

    def __setstate__(self, state):
        self._value = _none
        self._error = None
        self.on_computed = core_events.sinking_event_hook
        self.set_value(state)
        self._in_repr = False

none_future = ConstFuture(None)


class ErrorFuture(FutureBase):
    """Future that always raises the specified error."""
    def __init__(self, error):
        self._value = _none
        self._error = None
        self.on_computed = core_events.sinking_event_hook  # Simple performance optimization
        self.set_error(error)
        self._in_repr = False  # since we don't call super.__init__


