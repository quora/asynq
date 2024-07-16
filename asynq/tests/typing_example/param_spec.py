import asyncio

from asynq import asynq


@asynq()
def f(arg1: int, arg2: str):
    pass


async def main():
    await f.asyncio(1, "2")
    await f.asyncio(1, arg2="2")
    await f.asyncio(arg1=1, arg2="2")
    await f.asyncio(arg1="1", arg2=2)


asyncio.run(main())
