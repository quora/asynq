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


import asyncio
from typing import Any, Awaitable

from .batching import BatchItemBase
from .futures import ConstFuture

_asyncio_mode = 0


# asyncio_mode > 0 indicates that a synchronous call runs on an asyncio loop.
def is_asyncio_mode():
    return _asyncio_mode > 0


async def _gather(awaitables):
    """Gather awaitables, but wait all other awaitables to finish even if some of them fail."""

    if len(awaitables) == 0:
        return []

    tasks = [asyncio.ensure_future(awaitable) for awaitable in awaitables]

    # Wait for all tasks to finish, even if some of them fail.
    await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

    # mark exceptions are retrieved.
    for task in tasks:
        task.exception()

    return [task.result() for task in tasks]


async def resolve_awaitables(x: Any):
    """
    Resolve a possibly-nested collection of awaitables.
    """
    if isinstance(x, Awaitable):
        return await x
    if isinstance(x, ConstFuture):
        return x.value()
    if isinstance(x, BatchItemBase):
        raise RuntimeError("asynq BatchItem is not supported in asyncio mode")
    if isinstance(x, list):
        return await _gather([resolve_awaitables(item) for item in x])
    if isinstance(x, tuple):
        return tuple(await _gather([resolve_awaitables(item) for item in x]))
    if isinstance(x, dict):
        resolutions = await _gather([resolve_awaitables(value) for value in x.values()])
        return {key: resolution for (key, resolution) in zip(x.keys(), resolutions)}
    if x is None:
        return None

    raise TypeError("Unknown structured awaitable type: ", type(x))


class AsyncioMode:
    """
    This context manager is internally used by .asyncio() to indicate that we are in asyncio mode.

    In asyncio mode, .asynq() is redirected to .asyncio().
    A user does not have to use this context manager directly.
    """

    def __enter__(self):
        global _asyncio_mode
        _asyncio_mode += 1

    def __exit__(self, exc_type, exc_value, tb):
        global _asyncio_mode
        _asyncio_mode -= 1
