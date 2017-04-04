import asyncio
from asynq import batching3

from qcore.asserts import assert_eq


num_calls = 0


async def get_multi(keys):
    # it is important to have this sleep here
    # otherwise it would just continue to the next line right away
    await asyncio.sleep(0.1)
    global num_calls
    num_calls += 1
    return [key.swapcase() for key in keys]


class ABatch(batching3.Batch):
    async def flush(self, items):
        keys = [item.key for item in items]
        values = await get_multi(keys)
        return values


class ABatchItem(batching3.BatchItem):
    batch_cls = ABatch

    def __init__(self, key):
        super(ABatchItem, self).__init__()
        self.key = key


async def root():
    keys = ['manan', 'async', 'jelle', 'batch', 'martin']
    values = await asyncio.gather(*[middle1(key) for key in keys])
    values = await asyncio.gather(*[middle2(key) for key in values])
    return values


async def middle1(key):
    value = await leaf(key)
    value += '-1a'
    value = await leaf(value)
    return value


async def middle2(key):
    value = await leaf(key)
    value += '-2b'
    value = await leaf(value)
    return value


async def leaf(key):
    result = await ABatchItem(key).get()
    return result


def test():
    global num_calls
    num_calls = 0
    event_loop = asyncio.get_event_loop()
    result = event_loop.run_until_complete(root())
    assert_eq(['manan-1A-2B', 'async-1A-2B', 'jelle-1A-2B', 'batch-1A-2B', 'martin-1A-2B'], result)
    # this is mostly just making sure that get_multi is not called separately for each key.
    # In that case num_calls would be 20.
    assert_eq(6, num_calls)
