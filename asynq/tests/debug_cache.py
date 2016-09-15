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

from qcore import none, miss
from asynq import scheduler
from .caching import ExternalCacheBase, LocalCache


# Caches

class DebugExternalCache(ExternalCacheBase):
    def __init__(self, name):
        super(DebugExternalCache, self).__init__()
        self.name = name
        self._remote = LocalCache()
        self.run_scheduler_before_flush = False

    def _before_flush(self, batch):
        if self.run_scheduler_before_flush:
            print('Invoking scheduler recursively before batch flush.')
            scheduler.get_scheduler().await()

    def _flush(self, batch):
        print('%s batch %i:' % (self.name, batch.index))
        for item in batch.items:
            print('  %s' % str(item))
            if item.is_computed():
                # Item can be computed due to task cancellation
                continue
            try:
                method = self.__getattribute__('_flush_' + item.operation)
                item.set_value(method(item, *item.args))
            except BaseException as e:
                item.set_error(e)
                # No need to re-throw, since user will anyway
                # see it by accessing the value

    def _cancel_flush(self, batch):
        print('%s batch %i is cancelled.' % (self.name, batch.index))

    def _flush_get(self, item, key):
        value = self._remote.get(key)
        if self.use_local_cache:
            self._local.set(key, none if value is miss else value)
        return value

    def _flush_set(self, item, key, value):
        self._remote.set(key, value)
        return

    def _flush_clear(self, item):
        self._remote.clear()
        return


lc = LocalCache()
mc = DebugExternalCache('MC')
db = DebugExternalCache('DB')
db.use_local_cache = False


def reset_caches():
    print('Cache reset:')
    lc.clear()
    mc.cancel_flush()
    mc.clear()
    db.cancel_flush()
    db.clear()
    flush_caches()
    reset_cache_batch_indexes()
    print('Caches are clear.')
    print('')


def reset_cache_batch_indexes():
    mc._batch.index = 1
    db._batch.index = 1


def flush_caches():
    if mc._batch.items:
        mc.flush()
    if db._batch.items:
        db.flush()


def flush_and_clear_local_caches():
    flush_caches()
    lc.clear()
    mc._local.clear()
    db._local.clear()
