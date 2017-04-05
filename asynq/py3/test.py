import asyncio
import batching
from decorators import coroutine
from contexts import AsyncContext

from qcore.asserts import assert_eq


num_calls = 0


async def get_multi(keys):
    # it is important to have this sleep here
    # otherwise it would just continue to the next line right away
    print(keys)
    await asyncio.sleep(0.1)
    global num_calls
    num_calls += 1
    return [key.swapcase() for key in keys]


class ABatch(batching.Batch):
    async def flush(self, items):
        keys = [item.key for item in items]
        return await get_multi(keys)


class ABatchItem(batching.BatchItem):
    batch_cls = ABatch

    def __init__(self, key):
        super().__init__()
        self.key = key


flags = [False, False, False, False, False]


def check(*args):
    true_indexes = set(args)
    for i in range(len(flags)):
        if i in true_indexes:
            assert flags[i]
        else:
            assert not flags[i]


class Context(AsyncContext):
    def __init__(self, index):
        super().__init__()
        self.index = index

    def pause(self):
        flags[self.index] = False

    def resume(self):
        flags[self.index] = True


@coroutine()
def coro1(names):
    with Context(1):
        check(0, 1)
        values = yield [ABatchItem(name) for name in names]
        check(0, 1)
    check(0)
    names = [value + '-a' for value in values]
    values = yield [ABatchItem(name) for name in names]
    check(0)
    return values


@coroutine(pure=True)
def coro0(names):
    with Context(2):
        check(0, 2)
        values = yield [ABatchItem(name) for name in names]
        check(0, 2)
        with Context(3):
            check(0, 2, 3)
            names = [value + '-b' for value in values]
            values = yield [ABatchItem(name) for name in names]
            check(0, 2, 3)
            return values


@coroutine()
def sync_coro(names):
    with Context(4):
        check(0, 4)
        return names


@coroutine()
def coro2():
    with Context(0):
        check(0)
        val1, val2, val3, val4, val5 = yield (
            coro1.future(['key1', 'key2', 'key3', 'key4']),
            coro1.future(['key5', 'key6', 'key7']),
            coro0(['key8', 'key9', 'key10']),
            ABatchItem('key11'), sync_coro.future(['key12', 'key13'])
        )
        check(0)
    check()
    return val1, val2, val3, val4, val5


def test():
    ret = coro2()
    print(ret)


if __name__ == '__main__':
    test()
