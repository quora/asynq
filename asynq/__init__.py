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

Asynq is a framework for asynchronous programming in Python.

It supports futures, batching, and asynchronous contexts.

"""

from . import debug
from . import futures
from . import batching
from . import async_task
from . import scheduler
from . import decorators
from . import utils
from . import contexts
from . import scoped_value
from . import tools
from .futures import FutureBase, Future, FutureIsAlreadyComputed, none_future, ConstFuture, \
    ErrorFuture
from .batching import BatchBase, BatchItemBase, BatchingError, BatchCancelledError
from .async_task import AsyncTask, AsyncTaskCancelledError, AsyncTaskResult
from .scheduler import TaskScheduler, get_scheduler, get_active_task, set_scheduler, \
    AsyncTaskError
from .decorators import async, async_proxy, cached, has_async_fn, \
    is_pure_async_fn, is_async_fn, get_async_fn, get_async_or_sync_fn, async_call, \
    make_async_decorator
from .utils import await, result
from .contexts import NonAsyncContext, AsyncContext
from .scoped_value import AsyncScopedValue, async_override
from . import mock_ as mock
from .generator import END_OF_GENERATOR, async_generator, list_of_generator, take_first, Value

debug.sync = batching.sync
debug.attach_exception_hook()
