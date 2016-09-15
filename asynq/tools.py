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

Helper functions for use with asynq (similar to itertools).

"""

from .decorators import async, async_proxy, make_async_decorator
from .futures import ConstFuture
from .scheduler import get_scheduler
# we shouldn't use the return syntax in generators here so that asynq can be imported
# under Python versions that lack our patch to allow returning from generators
from .utils import result

from qcore import get_original_fn
from qcore.caching import set_cached_per_instance_getstate, get_args_tuple, get_kwargs_defaults
from qcore.inspection import getargspec
import functools
import itertools


@async()
def amap(function, sequence):
    """Equivalent of map() that takes an async map function.

    Returns a list.

    """
    result((yield [function.async(elt) for elt in sequence])); return


@async()
def afilter(function, sequence):
    """Equivalent of filter() that takes an async filter function.

    Returns a list.

    """
    if function is None:
        result(filter(None, sequence)); return
    should_include = yield [function.async(elt) for elt in sequence]
    result(list(itertools.compress(sequence, should_include))); return


@async()
def afilterfalse(function, sequence):
    """Equivalent of itertools.ifilterfalse() that takes an async filter function.

    Returns a list.

    """
    should_exclude = yield [function.async(elt) for elt in sequence]
    should_include = [not res for res in should_exclude]
    result(list(itertools.compress(sequence, should_include))); return


@async()
def asorted(iterable, key=None, reverse=False):
    """Equivalent of sorted() that takes an async key function.

    The cmp= argument of sorted() is not supported.

    Returns a list.

    """
    values = list(iterable)
    if key is None:
        keys = values
    else:
        keys = yield amap.async(key, values)
    # we need to use key= here because otherwise we will compare the values when the key are
    # equal, which would be a behavior difference between sorted() and asorted()
    pairs = sorted(zip(keys, values), key=lambda p: p[0], reverse=reverse)
    result([p[1] for p in pairs]); return


@async()
def amax(*args, **kwargs):
    """Async equivalent of max()."""
    key_fn = kwargs.pop('key', None)
    if kwargs:
        raise TypeError('amax() got an unexpected keyword argument')

    if len(args) == 0:
        raise TypeError('amax() expected 1 arguments, got 0')
    elif len(args) == 1:
        iterable = args[0]
    else:
        iterable = args

    if key_fn is None:
        result(max(iterable)); return

    # support generators
    if not isinstance(iterable, (list, tuple)):
        iterable = list(iterable)

    keys = yield amap.async(key_fn, iterable)
    max_pair = max(enumerate(iterable), key=lambda pair: keys[pair[0]])
    result(max_pair[1]); return


@async()
def amin(*args, **kwargs):
    """Async equivalent of min()."""
    key_fn = kwargs.pop('key', None)
    if kwargs:
        raise TypeError('amin() got an unexpected keyword argument')

    if len(args) == 0:
        raise TypeError('amin() expected 1 arguments, got 0')
    elif len(args) == 1:
        iterable = args[0]
    else:
        iterable = args

    if key_fn is None:
        result(min(iterable)); return

    # support generators
    if not isinstance(iterable, (list, tuple)):
        iterable = list(iterable)

    keys = yield amap.async(key_fn, iterable)
    max_pair = min(enumerate(iterable), key=lambda pair: keys[pair[0]])
    result(max_pair[1]); return


@async()
def asift(pred, items):
    """Sifts a list of items into those that meet the predicate and those that don't."""
    yes = []
    no = []
    results = yield [pred.async(item) for item in items]
    for item, yesno in zip(items, results):
        if yesno:
            yes.append(item)
        else:
            no.append(item)
    result((yes, no)); return


def acached_per_instance():
    """Async equivalent of core.caching.cached_per_instance().

    Unlike l0cache, the cached value is stored in the instance so that it gets
    garbage collected together with the instance.

    The cached values are not stored when the object is pickled.

    """
    def cache_fun(fun):
        argspec = getargspec(get_original_fn(fun))
        arg_names = argspec.args[1:]  # remove self
        async_fun = fun.async
        kwargs_defaults = get_kwargs_defaults(argspec)

        def cache_key(args, kwargs):
            args = get_args_tuple(args, kwargs, arg_names, kwargs_defaults)
            return (fun.__module__, fun.__name__, args)

        @async_proxy()
        @functools.wraps(fun)
        def new_fun(self, *args, **kwargs):
            try:
                cache = self.__lib_cache
            except AttributeError:
                cache = self.__lib_cache = {}
                set_cached_per_instance_getstate(self)

            k = cache_key(args, kwargs)
            try:
                return ConstFuture(cache[k])
            except KeyError:
                def callback(task):
                    cache[k] = task.value()

                task = async_fun(self, *args, **kwargs)
                task.on_computed.subscribe(callback)
                return task
        return new_fun
    return cache_fun


@async()
def call_with_context(context, fn, *args, **kwargs):
    """Calls fn in the given with context.

    This is useful if you need to call two functions at once, but only one should be called in the
    context. For example:

        important, not_important = yield (
            get_important.async(oid),
            call_with_context.async(a.livenode.dep.IgnoreDependencies(), get_not_important, oid).
        )

    """
    with context:
        result((yield fn.async(*args, **kwargs))); return


def deduplicate():
    """Decorator that (mostly) ensures that no two identical instances of a task run concurrently.

    This is useful in situations like this:

        @async()
        def should_filter_object(oid, uid):
            data = yield get_data_for_user.async(uid)
            ...

        @async()
        def filter_objects(oids, uid):
            ... = yield [should_filter_object.async(oid, uid) for oid in oids]

    where get_data_for_user is cached (e.g. in memcache or l0cache). Without the deduplicate
    decorator, this may end up calling the body of the get_data_for_user function multiple times,
    despite the caching, because a second async task may enter the body while the first one is
    still active.

    This decorator will *not* deduplicate tasks that are scheduled on different asynq schedulers. In
    practice, this can happen when the await recursion depth (the number of async tasks that call
    .value() on async tasks) reaches 10 (see scheduler.py). If we attempt to deduplicate across
    multiple schedulers, the scheduler may end up being blocked on a future that is owned by a
    different scheduler, and this will lead the scheduler to fail with "No task to continue or batch
    to flush".

    """
    def decorator(fun):
        original_fn = get_original_fn(fun)
        argspec = getargspec(original_fn)
        arg_names = argspec.args
        kwargs_defaults = get_kwargs_defaults(argspec)
        tasks = {}
        async_fn = fun.async

        def wrapper_fn(*args, **kwargs):
            # see docstring for why scheduler is in the cache key
            cache_key = get_args_tuple(args, kwargs, arg_names, kwargs_defaults), get_scheduler()

            try:
                return tasks[cache_key]
            except KeyError:
                task = async_fn(*args, **kwargs)

                def callback(task):
                    del tasks[cache_key]

                tasks[cache_key] = task
                task.on_computed.subscribe(callback)
                return task

        return make_async_decorator(fun, wrapper_fn, 'deduplicate')
    return decorator
