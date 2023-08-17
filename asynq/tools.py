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

"""

Helper functions for use with asynq (similar to itertools).

"""

from .contexts import AsyncContext
from .decorators import asynq, async_call, AsyncDecorator, AsyncDecoratorBinder

from qcore import get_original_fn, utime
from qcore.caching import get_args_tuple, get_kwargs_defaults, LRUCache
from qcore.events import EventHook
from qcore.errors import reraise, prepare_for_reraise
from qcore.decorators import decorate
import functools
import inspect
import itertools
import weakref
import threading
import time


@asynq()
def amap(function, sequence):
    """Equivalent of map() that takes an async map function.

    Returns a list.

    """
    return (yield [function.asynq(elt) for elt in sequence])


@asynq()
def afilter(function, sequence):
    """Equivalent of filter() that takes an async filter function.

    Returns a list.

    """
    if function is None:
        return list(filter(None, sequence))

    # Make sure generators work correctly (we'll iterate over them twice).
    sequence = list(sequence)
    should_include = yield [function.asynq(elt) for elt in sequence]
    return list(itertools.compress(sequence, should_include))


@asynq()
def afilterfalse(function, sequence):
    """Equivalent of itertools.ifilterfalse() that takes an async filter function.

    Returns a list.

    """
    sequence = list(sequence)
    should_exclude = yield [function.asynq(elt) for elt in sequence]
    should_include = [not res for res in should_exclude]
    return list(itertools.compress(sequence, should_include))


@asynq()
def asorted(iterable, key=None, reverse=False):
    """Equivalent of sorted() that takes an async key function.

    The cmp= argument of sorted() is not supported.

    Returns a list.

    """
    values = list(iterable)
    if key is None:
        keys = values
    else:
        keys = yield amap.asynq(key, values)
    # we need to use key= here because otherwise we will compare the values when the key are
    # equal, which would be a behavior difference between sorted() and asorted()
    pairs = sorted(zip(keys, values), key=lambda p: p[0], reverse=reverse)
    return [p[1] for p in pairs]


@asynq()
def amax(*args, **kwargs):
    """Async equivalent of max()."""
    key_fn = kwargs.pop("key", None)
    if kwargs:
        raise TypeError("amax() got an unexpected keyword argument")

    if len(args) == 0:
        raise TypeError("amax() expected 1 arguments, got 0")
    elif len(args) == 1:
        iterable = args[0]
    else:
        iterable = args

    if key_fn is None:
        return max(iterable)

    # support generators
    if not isinstance(iterable, (list, tuple)):
        iterable = list(iterable)

    keys = yield amap.asynq(key_fn, iterable)
    max_pair = max(enumerate(iterable), key=lambda pair: keys[pair[0]])
    return max_pair[1]


@asynq()
def amin(*args, **kwargs):
    """Async equivalent of min()."""
    key_fn = kwargs.pop("key", None)
    if kwargs:
        raise TypeError("amin() got an unexpected keyword argument")

    if len(args) == 0:
        raise TypeError("amin() expected 1 arguments, got 0")
    elif len(args) == 1:
        iterable = args[0]
    else:
        iterable = args

    if key_fn is None:
        return min(iterable)

    # support generators
    if not isinstance(iterable, (list, tuple)):
        iterable = list(iterable)

    keys = yield amap.asynq(key_fn, iterable)
    max_pair = min(enumerate(iterable), key=lambda pair: keys[pair[0]])
    return max_pair[1]


@asynq()
def asift(pred, items):
    """Sifts a list of items into those that meet the predicate and those that don't."""
    yes = []
    no = []
    results = yield [pred.asynq(item) for item in items]
    for item, yesno in zip(items, results):
        if yesno:
            yes.append(item)
        else:
            no.append(item)
    return (yes, no)


def acached_per_instance():
    """Async equivalent of qcore.caching.cached_per_instance().

    Unlike l0cache, the cached value is stored in the instance so that it gets
    garbage collected together with the instance.

    The cached values are not stored when the object is pickled.

    """

    def cache_fun(fun):
        argspec = inspect.getfullargspec(get_original_fn(fun))
        arg_names = argspec.args[1:] + argspec.kwonlyargs  # remove self
        async_fun = fun.asynq
        kwargs_defaults = get_kwargs_defaults(argspec)
        cache = {}

        def cache_key(args, kwargs):
            return get_args_tuple(args, kwargs, arg_names, kwargs_defaults)

        def clear_cache(instance_key, ref):
            del cache[instance_key]

        @asynq()
        @functools.wraps(fun)
        def new_fun(self, *args, **kwargs):
            instance_key = id(self)
            if instance_key not in cache:
                ref = weakref.ref(self, functools.partial(clear_cache, instance_key))
                cache[instance_key] = (ref, {})
            instance_cache = cache[instance_key][1]

            k = cache_key(args, kwargs)
            try:
                return instance_cache[k]
            except KeyError:
                value = yield async_fun(self, *args, **kwargs)
                instance_cache[k] = value
                return value

        # just so unit tests can check that this is cleaned up correctly
        new_fun.__acached_per_instance_cache__ = cache
        return new_fun

    return cache_fun


def alru_cache(maxsize=128, key_fn=None):
    """Async equivalent of qcore.caching.lru_cache().

    maxsize is the number of different keys cache can accommodate.
    key_fn is the function that builds key from args. The default key function
    creates a tuple out of args and kwargs. If you use the default it acts the same
    as functools.lru_cache (except with async).

    Possible use cases of key_fn:
    - Your cache key is very large, so you don't want to keep the whole key in memory.
    - The function takes some arguments that don't affect the result.

    """

    def decorator(fn):
        cache = LRUCache(maxsize)
        argspec = inspect.getfullargspec(get_original_fn(fn))
        arg_names = argspec.args[1:] + argspec.kwonlyargs  # remove self
        async_fun = fn.asynq
        kwargs_defaults = get_kwargs_defaults(argspec)

        cache_key = key_fn
        if cache_key is None:

            def cache_key(args, kwargs):
                return get_args_tuple(args, kwargs, arg_names, kwargs_defaults)

        @asynq()
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            key = cache_key(args, kwargs)
            try:
                return cache[key]
            except KeyError:
                value = yield async_fun(*args, **kwargs)
                cache[key] = value
                return value

        return wrapper

    return decorator


def alazy_constant(ttl=0):
    """Async implementation of qcore.caching.lazy_constant.

    If ttl is given, the cached value is automatically dirtied after that many
    microseconds. If ttl is zero or not given, the cached value lasts for the
    lifetime of the process, or until .dirty() is called.

    """

    def decorator(fn):
        @asynq()
        @functools.wraps(fn)
        def wrapper():
            if (wrapper.alazy_constant_refresh_time == 0) or (
                (ttl != 0) and (wrapper.alazy_constant_refresh_time < utime() - ttl)
            ):
                wrapper.alazy_constant_cached_value = yield fn.asynq()
                wrapper.alazy_constant_refresh_time = utime()
            return wrapper.alazy_constant_cached_value

        def dirty():
            wrapper.alazy_constant_refresh_time = 0

        wrapper.dirty = dirty
        wrapper.alazy_constant_refresh_time = 0
        wrapper.alazy_constant_cached_value = None
        return wrapper

    return decorator


def aretry(exception_cls, max_tries=10, sleep=0.05):
    """Decorator for retrying an async function if it throws an exception.

    exception_cls - an exception type or a parenthesized tuple of exception types
    max_tries - maximum number of times this function can be executed
    sleep - number of seconds to sleep between function retries

    """
    assert max_tries > 0, "max_tries (%d) should be a positive integer" % max_tries

    def decorator(fn):
        @asynq()
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for i in range(max_tries):
                try:
                    ret = yield fn.asynq(*args, **kwargs)
                    return ret
                except exception_cls:
                    if i + 1 == max_tries:
                        raise
                    time.sleep(sleep)

        # so that qcore.inspection.get_original_fn can retrieve the original function
        wrapper.original_fn = fn
        return wrapper

    return decorator


@asynq()
def call_with_context(context, fn, *args, **kwargs):
    """Calls fn in the given with context.

    This is useful if you need to call two functions at once, but only one should be called in the
    context. For example:

        important, not_important = yield (
            get_important.asynq(oid),
            call_with_context.asynq(a.livenode.dep.IgnoreDependencies(), get_not_important, oid).
        )

    """
    with context:
        return (yield fn.asynq(*args, **kwargs))


class DeduplicateDecoratorBinder(AsyncDecoratorBinder):
    def dirty(self, *args, **kwargs):
        if self.instance is None:
            self.decorator.dirty(*args, **kwargs)
        else:
            self.decorator.dirty(self.instance, *args, **kwargs)


class DeduplicateDecorator(AsyncDecorator):
    binder_cls = DeduplicateDecoratorBinder
    tasks = {}

    def __init__(self, fn, task_cls, keygetter):
        AsyncDecorator.__init__(self, fn, task_cls)
        self.keygetter = keygetter

    def cache_key(self, args, kwargs):
        return self.keygetter(args, kwargs), threading.current_thread(), id(self.fn)

    def asynq(self, *args, **kwargs):
        cache_key = self.cache_key(args, kwargs)

        try:
            task = self.tasks[cache_key]
        except KeyError:
            task = self.fn.asynq(*args, **kwargs)

            def callback(task):
                self.tasks.pop(cache_key, None)

            self.tasks[cache_key] = task
            task.on_computed.subscribe(callback)
            return task
        else:
            if task.running:
                # If the task is currently executing, don't return it; asynq
                # will try to send another value into the generator and fail
                # with "ValueError: generator is already executing"
                return self.fn.asynq(*args, **kwargs)
            return task

    def dirty(self, *args, **kwargs):
        cache_key = self.cache_key(args, kwargs)
        self.tasks.pop(cache_key, None)


def deduplicate(keygetter=None):
    """Decorator that ensures that no two identical instances of a task run concurrently.

    This is useful in situations like this:

        @asynq()
        def should_filter_object(oid, uid):
            data = yield get_data_for_user.asynq(uid)
            ...

        @asynq()
        def filter_objects(oids, uid):
            ... = yield [should_filter_object.asynq(oid, uid) for oid in oids]

    where get_data_for_user is cached (e.g. in memcache or l0cache). Without the deduplicate
    decorator, this may end up calling the body of the get_data_for_user function multiple times,
    despite the caching, because a second async task may enter the body while the first one is
    still active.

    You can also call dirty on a deduplicated function to remove a cached async task with the
    corresponding args and kwargs. This is useful if a deduplicating function ends up calling
    itself with the same args and kwargs, either directly or deeper in the call stack.

    Multiple instances of a task may be created if the first instance of the task is running
    while the function is invoked again. This can happen if the function synchronously invokes
    asynq code that ends up calling the original function. See the test case in test_tools.py
    involving the deduplicated_recursive() function for an example.

    """

    def decorator(fun):
        _keygetter = keygetter
        if _keygetter is None:
            original_fn = get_original_fn(fun)
            argspec = inspect.getfullargspec(original_fn)
            arg_names = argspec.args + argspec.kwonlyargs
            kwargs_defaults = get_kwargs_defaults(argspec)
            _keygetter = lambda args, kwargs: get_args_tuple(
                args, kwargs, arg_names, kwargs_defaults
            )

        return decorate(DeduplicateDecorator, fun.task_cls, _keygetter)(fun)

    return decorator


class AsyncTimer(AsyncContext):
    """Simple async-aware timer class.

    Use this to find out how long a block of code takes within an async task. If
    other tasks run interspersed with the task in which this is used, time spent
    executing those tasks will not be counted. The result (in microseconds) will
    be available as the total_time attribute on the context object after exiting
    the context.

    The total_time attribute may have a nonzero value during the context if any
    yields were performed. Because of optimizations that may be done in the
    future to how contexts work between tasks, the value shouldn't be trusted
    until exiting the context.

    Usage example:
        @asynq()
        def potentially_slow_function(x):

            with AsyncTimer() as t:
                yield do_a_lot_of_work.asynq(x)
                # don't use t.total_time here!

            report_time_for_x(x, t.total_time)

        yield [potentially_slow_function(x) for x in all_x_values]

    """

    def __init__(self):
        self.total_time = 0
        self._last_start_time = None

    def resume(self):
        self._last_start_time = utime()

    def pause(self):
        self.total_time += utime() - self._last_start_time


class AsyncEventHook(EventHook):
    """EventHook that supports async handlers.

    When the event triggers, all the async handlers will be invoked asynchronously.

    All non-async handlers will be invoked normally (same as EventHook).

    """

    @asynq()
    def trigger(self, *args):
        yield [async_call.asynq(handler, *args) for handler in self]

    @asynq()
    def safe_trigger(self, *args):
        wrapped_handlers = [self._create_safe_wrapper(handler) for handler in self]
        results = yield [
            wrapped_handler.asynq(*args) for wrapped_handler in wrapped_handlers
        ]
        for error in filter(None, results):
            reraise(error)

    @staticmethod
    def _create_safe_wrapper(handler):
        @asynq()
        def wrapped(*args):
            error = None
            try:
                yield async_call.asynq(handler, *args)
            except BaseException as e:
                prepare_for_reraise(e)
                error = e
            return error

        return wrapped
