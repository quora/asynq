import asyncio
from typing import Any, Generator

from asynq import asynq


@asynq()
def noop():
    return


# case: no annotation + no yield
@asynq()
def f1():
    return 100


# case: no annotation + yield
@asynq()
def f2():
    yield noop.asynq()
    return 100


# case: annotation + no yield
@asynq()
def f3() -> int:
    return 300


# case: annotation + yield
@asynq()
def f4() -> Generator[Any, Any, int]:
    yield noop()
    return 300


async def main():
    _: int = await f1.asyncio()
    _: str = await f1.asyncio()

    _: int = await f2.asyncio()
    _: str = await f2.asyncio()

    _: int = await f3.asyncio()
    _: str = await f3.asyncio()

    _: int = await f4.asyncio()
    _: str = await f4.asyncio()


asyncio.run(main())
