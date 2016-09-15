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

import sys
import threading
import qcore.inspection as core_inspection

from . import debug
from . import futures
from . import _debug


__traceback_hide__ = True

_debug_options = _debug.options


class BatchingError(Exception):
    pass


class BatchCancelledError(BatchingError):
    pass


class BatchBase(futures.FutureBase):
    """Abstract base class describing a batch of operations."""
    def __init__(self):
        futures.FutureBase.__init__(self)  # Cython doesn't support super(...)
        self.items = []

    def is_flushed(self):
        return self.is_computed()

    def is_cancelled(self):
        return self.is_computed() and self.error() is not None

    def is_empty(self):
        return len(self.items) == 0

    def get_priority(self):
        """Returns batch flush priority.
        The higher priority, the earlier batch is flushed.

        By default it returns ``(0, len(self.items))`` tuple;
        0 here is base batch priority (zero by default).

        :return: batch flush priority.

        """
        return 0, len(self.items)

    def flush(self):
        """Flushes the batch.

        Almost the same as ``self.value()``, but:

        * this method doesn't throw an error even if underlying batch
          flush actually completed with an error
        * on the other hand, subsequent flush throws an error.

        So this method is intended to be called by schedulers:

        * They must flush each batch just once
        * They don't care (and moreover, shouldn't know) about actual
          flush errors. These errors will be anyway re-thrown later -
          on attempt to access values of underlying batch items.

        """
        if self.is_computed():
            raise BatchingError('Batch is already flushed or cancelled.')
        if _debug_options.DUMP_FLUSH_BATCH:
            debug.write('@async: -> batch flush:')
            self.dump(4)
            if _debug_options.DUMP_STACK:
                debug.dump_stack()
        try:
            self.error()  # Makes future to compute w/o raising an error
        finally:
            if _debug_options.DUMP_FLUSH_BATCH:
                debug.write('@async: <- batch flushed: %s' % debug.str(self))

    def cancel(self, error=None):
        """Cancels the batch.

        It's a public ``_cancel()`` wrapper enforcing
        additional safety properties.

        """
        if self.is_computed():
            return  # Cancel must never raise an error
        if error is None:
            error = BatchCancelledError()
        self.set_error(error)

    def _compute(self):
        self._try_switch_active_batch()
        try:
            self._flush()
            self.set_value(None)
        except BaseException as error:
            if not self.is_computed():
                self.set_error(error)
            raise  # Must re-throw to properly implement FutureBase API

    def _computed(self):
        # The purpose of this overridden method is to ensure that
        # all items are computed before on_computed event is raised.
        self._try_switch_active_batch()
        error = self.error()
        cancelled = error is not None
        if cancelled:
            self._cancel()
        for item in self.items:
            if not item.is_computed():
                # We must ensure all batch items are computed
                item.set_error(
                    error if cancelled
                    else AssertionError("Value of this item wasn't set on batch flush."))
        futures.FutureBase._computed(self)  # Cython doesn't support super(...)

    def _flush(self):
        """A protected method that must be override to implement batch flush.
        Normally it should simply forward the call to appropriate service
        (cache, DB, etc.), that must execute the batch (i.e. ensure each
        batch item will be able to acquire its value on subsequent attempt)
        and set its current batch to the newly created one.

        """
        raise NotImplementedError()

    def _cancel(self):
        """A protected method that must be override to implement batch cancellation.
        Normally it should simply forward the call to appropriate service
        (cache, DB, etc.), that must discard the current batch in this case.

        This method is optional to implement: _computed implementation
        anyway ensures that all items are computed as well, but
        you can add some additional logic here, if you want to.

        """
        pass

    def _try_switch_active_batch(self):
        """This protected method must be overridden to switch to a new active
        batch of this type.

        Must never throw an error.

        """
        raise NotImplementedError()

    def __str__(self):
        return '%s (%s, %i items)' % (
            core_inspection.get_full_name(type(self)),
            'cancelled' if self.is_cancelled() else
                'flushed' if self.is_flushed() else 'pending',
            len(self.items))

    def dump(self, indent=0):
        debug.write(debug.str(self), indent)
        debug.write('Priority: %s' % debug.repr(self.get_priority()), indent + 1)
        if self.items:
            debug.write('Items:', indent + 1)
            for item in self.items:
                item.dump(indent + 2)
        else:
            debug.write('No items.', indent + 1)


class BatchItemBase(futures.FutureBase):
    """Abstract base class describing batch item.
    Batch items are futures providing result
    of a particular cache operation.

    """
    def __init__(self, batch):
        super(BatchItemBase, self).__init__()
        assert not batch.is_flushed(), "can't add an item to the batch that is already flushed"
        self.batch = batch
        self.index = len(batch.items)
        batch.items.append(self)

    def make_dependency(self, task, scheduler):
        task._dependencies.add(self)
        self.on_computed.subscribe(task._remove_dependency)
        scheduler.schedule_batch(self.batch)

    def _compute(self):
        """This method ensures the value is available
        by flushing the batch, if necessary.

        """
        if not self.batch.is_flushed():
            self.batch.flush()


class DebugBatchItem(BatchItemBase):
    """Debug batch item used to sync async execution."""

    def __init__(self, batch_name='default', result=None):
        global _debug_batch_state
        batch = _debug_batch_state.batches.setdefault(batch_name, DebugBatch(batch_name))
        super(DebugBatchItem, self).__init__(batch)
        self._result = result


class DebugBatch(BatchBase):
    """Debug batch used to sync async execution."""

    def __init__(self, name='default', index=0):
        super(DebugBatch, self).__init__()
        self.name = name
        self.index = index

    def _try_switch_active_batch(self):
        if _debug_batch_state.batches.get(self.name, None) is self:
            _debug_batch_state.batches[self.name] = DebugBatch(self.name, self.index + 1)

    def _flush(self):
        global _debug_batch_state
        if _debug_options.DUMP_SYNC:
            debug.write("@async.debug.sync: flushing batch %s (%i)" % (debug.repr(self.name), self.index))
        for item in self.items:
            item.set_value(item._result)

    def _cancel(self):
        global _debug_batch_state
        if _debug_options.DUMP_SYNC:
            debug.write("@async.debug.sync: cancelling batch %s (%i)" % (debug.repr(self.name), self.index))


class LocalDebugBatchState(threading.local):
    def __init__(self):
        super(LocalDebugBatchState, self).__init__()
        self.batches = {}

_debug_batch_state = LocalDebugBatchState()
globals()['_debug_batch_state'] = _debug_batch_state


def sync(tag='default'):
    return DebugBatchItem('sync-' + tag)

