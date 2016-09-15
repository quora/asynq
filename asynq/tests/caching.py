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

from qcore import EventHook, miss, none
from asynq import BatchBase, BatchItemBase, ConstFuture


class CacheBase(object):
    '''
    Abstract base class for any cache.
    '''
    def __init__(self):
        self.on_dependency = EventHook()

    def get(self, key):
        '''
        Gets the value from cache.
        Returns miss (as future), if there is no cached value.
        Normally should return a FutureBase descendant -
        local caches are the only exception from this rule
        (they return values directly).
        '''
        raise NotImplementedError()

    def set(self, key, value):
        '''
        Stores the value in the cache.
        If passed value is miss, the value is evicted from cache.
        The actual operation is normally batched,
        so cache implementation should ensure consistency
        for subsequent get operations (i.e. any stored value
        must be instantly available for get operations,
        even although actual operation was batched).
        Normally returns a future returning None or raising
        an error.
        '''
        raise NotImplementedError()

    def _dependency(self, key):
        self.on_dependency(key)


class LocalCache(CacheBase):
    '''
    Local cache implementation (dictionary-based).
    Since all operations can be performed immediately
    by this case, it never returns futures (so it doesn't
    employ batching as well).
    '''
    def __init__(self):
        super(LocalCache, self).__init__()
        self._storage = {}

    def get(self, key):
        self._dependency(key)
        return self._storage.get(key, miss)

    def set(self, key, value):
        self._dependency(key)
        if (value is miss):
            if key in self._storage:
                del self._storage[key]
        else:
            self._storage[key] = value
        return

    def clear(self):
        self._storage.clear()
        return


class ExternalCacheBatch(BatchBase):
    '''
    Batch implementation for external caches.
    '''
    def __init__(self, cache, index):
        super(ExternalCacheBatch, self).__init__()
        self.cache = cache
        self.index = index  # Batch index. Just for debugging purposes.

    def _try_switch_active_batch(self):
        if self.cache._batch is self:
            self.cache._batch = ExternalCacheBatch(self.cache, self.index + (1 if self.items else 0))

    def _flush(self):
        self.cache._before_flush(self)
        self.cache._flush(self)

    def _cancel(self):
        self.cache._cancel_flush(self)


class ExternalCacheBatchItem(BatchItemBase):
    '''
    Batch item implementation for external caches.
    '''
    def __init__(self, batch, operation, *args):
        super(ExternalCacheBatchItem, self).__init__(batch)
        self.operation = operation
        self.args = args

    def __str__(self):
        return '%s (%s)' % (self.operation, ', '.join(map(lambda i: repr(i), self.args)))


class ExternalCacheBase(CacheBase):
    '''
    Abstract base class for external caches (memcached, etc.).
    '''
    def __init__(self):
        super(ExternalCacheBase, self).__init__()
        self._local = LocalCache()
        self._batch = ExternalCacheBatch(self, 1)
        self.use_local_cache = True

    def get(self, key):
        self._dependency(key)
        value = self._local.get(key)
        if value is miss:  # No item is cached locally
            return self._create_batch_item('get', key)
        return ConstFuture(miss if value is none else value)

    def set(self, key, value):
        self._dependency(key)
        if self.use_local_cache:
            self._local.set(key, none if value is miss else value)
        return self._create_batch_item('set', key, value)

    def clear(self):
        self.clear_local_cache()
        return self._create_batch_item('clear')

    def flush(self):
        return self._batch.flush()

    def cancel_flush(self):
        return self._batch.cancel()

    def clear_local_cache(self):
        self._local.clear()

    def _before_flush(self, batch):
        pass

    def _flush(self, batch):
        # Actual batch flush implementation
        raise NotImplementedError()

    def _cancel_flush(self, batch):
        # Actual batch cancel implementation
        pass

    def _create_batch_item(self, operation, *args):
        return ExternalCacheBatchItem(self._batch, operation, *args)
