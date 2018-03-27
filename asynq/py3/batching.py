import asyncio
import threading


class Batch:
    def __init__(self):
        self.items = set()
        self.lock = asyncio.Lock()

    async def enqueue(self, item):
        self.items.add(item)
        async with self.lock:
            if item not in self.items:
                return
            items = self.items.copy()
            values = await self.flush(items)
            for i, value in zip(items, values):
                i.set_value(value)
            self.items = self.items.difference(items)

    async def flush(self, items):
        raise NotImplementedError()


class BatchItem:
    batch_cls = None
    def __init__(self):
        assert self.batch_cls is not None, 'BatchItem classes must define the batch_cls class variable'
        self.batch = _local_state.get_batch(self.batch_cls)
        self._value = None

    def set_value(self, value):
        self._value = value

    def value(self):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.future())

    async def future(self):
        await self.batch.enqueue(self)
        return self._value


class ConstFuture:
    def __init__(self, value):
        self._value = value

    async def future(self):
        return self._value

    def value(self):
        return self._value


class LocalState(threading.local):
    def __init__(self):
        super().__init__()
        self.batches = {}

    def get_batch(self, batch_cls):
        if batch_cls not in self.batches:
            self.batches[batch_cls] = batch_cls()
        return self.batches[batch_cls]


_local_state = LocalState()
