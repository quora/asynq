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

import six

from .futures import FutureBase
from . import async_task
from . import scheduler
from . import _debug


_debug_options = _debug.options


def execute_on_separate_scheduler(async_pull_fn, contexts, args=(), kwargs={}):
    current_scheduler = scheduler.get_scheduler()
    new_scheduler = current_scheduler._clone()
    try:
        with new_scheduler.activate(True, False):
            task = async_task.AsyncTask(async_pull_fn(*args, **kwargs), async_pull_fn, args, kwargs)
            # The above tasks should have no contexts right now (because it is created in a vacuum).
            # We filter the contexts that are already active because they will remain active
            # throughout the execution of this task. Not filtering them would result in calling
            # resume on them twice in a row (which most async_contexts do not support).
            task._contexts = [ctx for ctx in contexts if not ctx._is_active]
            new_scheduler.await([task])
    finally:
        current_scheduler._accept_changes(new_scheduler)
    assert task.is_computed(), 'the task should have been computed at this point'
    return task.value()


def await(*args):
    """Runs all the tasks specified in args,
    and finally returns args unwrapped.

    """
    return _await(args)


def _await(args):
    tasks = set()
    _extract_tasks(args, tasks)
    scheduler.get_scheduler()._execute(True, tasks)
    return _unwrap(args)


def _unwrap(args):
    l = len(args)
    assert l != 0, "No arguments to unwrap."
    if l == 1:
        return async_task.unwrap(args[0])  # Special case: no need to return tuple here
    return async_task.unwrap(args)


def result(value):
    """An async equivalent for return for async methods: result(x); return."""
    assert not isinstance(value, FutureBase), \
        "You probably forgot to yield this value before returning"
    raise async_task.AsyncTaskResult(value)


# Private part

def _extract_tasks(value, result):
    if value is None:
        pass
    elif isinstance(value, async_task.AsyncTask):
        result.add(value)
    elif type(value) is tuple or type(value) is list:
        for item in value:
            _extract_tasks(item, result)
    elif type(value) is dict:
        for item in six.itervalues(value):
            _extract_tasks(item, result)
    elif isinstance(value, FutureBase):
        pass
    else:
        raise TypeError(
            "Cannot process an object of type '%s': only futures and None are allowed."
            % type(value)
        )
