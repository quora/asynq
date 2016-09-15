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

Simple example of asynq-based batching support for a memcache client.

Requires the python-memcached library to be installed.

"""

from asynq import async, async_proxy, BatchBase, BatchItemBase, result
import core
import itertools
import memcache

MISS = core.MarkerObject('miss')


class Client(object):
    """Memcache client class supporting asynchronous, batched operations.

    Example usage:

        client = Client(['127.0.0.1:11211'])

        @client.cached('user_name')
        def name_of_user(uid):
            return database_query(...)

        @client.cached('answer_author')
        def author_of_answer(aid):
            return database_query(...)

        @async()
        def all_author_names(aids):
            "Returns names of authors for all of the given aids."
            uids = yield [author_of_answer.async(aid) for aid in aids]
            names = yield [name_of_user.async(uid) for uid in uids]
            result(names); return

    """
    def __init__(self, servers):
        # keep a reference to the current batch and to the underlying library's client object
        self.batch = _MCBatch(self)
        self._mc_client = memcache.Client(servers)

    def _make_batch_item(self, command, args):
        return _MCBatchItem(self.batch, command, args)

    @async_proxy()
    def get(self, key):
        """Gets the value of key. May be called asynchronously."""
        return self._make_batch_item('get', (key,))

    @async_proxy()
    def set(self, key, value):
        """Sets the value of key. May be called asynchronously."""
        return self._make_batch_item('set', (key, value))

    def sync_get_multi(self, keys):
        """Synchronously retrieves the values for the given keys."""
        return self._mc_client.get_multi(keys)

    def sync_set_multi(self, items):
        """Synchronously sets all the keys in a {key: value} dictionary."""
        return self._mc_client.set_multi(items)

    def cached(self, key_prefix):
        """Decorator to wrap a function so that its result is cached in memcache.

        Example usage:

            @client.cached('my_key_prefix')
            def cached_function(a, b):
                return expensive_computation(a, b)

        In calling code:

            value = yield cached_function.async(a, b)

        """
        def decorator(fn):
            @async()
            def wrapped(*args):
                key = key_prefix + ':' + ':'.join(map(str, args))
                value = yield self.get.async(key)
                if value is MISS:
                    # possible enhancement: make it possible for the inner function to be async
                    value = fn(*args)
                    # there is a race condition here since somebody else could have set the key
                    # while we were computing the value, but don't worry about that for now
                    yield self.set.async(key, value)
                result(value); return
            return wrapped
        return decorator


class _MCBatch(BatchBase):
    def __init__(self, client):
        self.client = client
        super(_MCBatch, self).__init__()

    def _try_switch_active_batch(self):
        if self.client.batch is self:
            self.client.batch = _MCBatch(self.client)

    def _flush(self):
        item_groups = itertools.groupby(self.items, lambda i: i.cmd)
        for group, items in item_groups:
            items = list(items)
            if group == 'get':
                keys = {item.args[0] for item in items}
                values = self.client.sync_get_multi(keys)
                for item in items:
                    key = item.args[0]
                    if key in values:
                        item.set_value(values[key])
                    else:
                        item.set_value(MISS)
            elif group == 'set':
                args = {item.args[0]: item.args[1] for item in items}
                not_stored = set(self.client.sync_set_multi(args))
                for item in items:
                    item.set_value(item.args[0] not in not_stored)


class _MCBatchItem(BatchItemBase):
    def __init__(self, batch, cmd, args):
        super(_MCBatchItem, self).__init__(batch)
        self.cmd = cmd
        self.args = args
