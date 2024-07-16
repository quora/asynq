from typing import Any, Generator, TYPE_CHECKING
from typing_extensions import assert_type
from asynq.decorators import async_call, lazy, asynq
from asynq.futures import FutureBase


def test_lazy() -> None:
    @lazy
    def lazy_func(x: int) -> str:
        return str(x)

    if TYPE_CHECKING:
        assert_type(lazy_func(1), FutureBase[str])


def test_dot_asyncio() -> None:
    @asynq()
    def non_generator(x: int) -> str:
        return str(x)

    @asynq()
    def generator(x: int) -> Generator[Any, Any, str]:
        yield None
        return str(x)

    async def caller() -> None:
        # This doesn't work, apparently due to a mypy bug
        assert_type(await generator.asyncio(1), str)  # type: ignore[assert-type]
        assert_type(await non_generator.asyncio(1), str)  # type: ignore[assert-type]

        await non_generator.asyncio()  # type: ignore[call-arg]
        await generator.asyncio()  # type: ignore[call-arg]


def test_async_call() -> None:
    def f(x: int) -> str:
        return str(x)

    async_call(f, 1)
    if TYPE_CHECKING:
        async_call(f, 1, task_cls=FutureBase)  # TODO: this should be an error
