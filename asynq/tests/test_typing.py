from typing import Any, Generator, TYPE_CHECKING, TypeVar
from typing_extensions import assert_type
from asynq.decorators import async_call, lazy, asynq
from asynq.futures import FutureBase
from asynq.tools import amax

_T = TypeVar("_T")


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

    @asynq()
    def generic(x: _T) -> _T:
        return x

    class Obj:
        @asynq()
        def method(self, x: str) -> int:
            return int(x)

    async def caller(x: str, obj: Obj) -> None:
        assert_type(await generator.asyncio(1), Any)  # TODO: str
        assert_type(await non_generator.asyncio(1), Any)  # TODO: str
        assert_type(await generic.asyncio(x), Any)  # TODO: str
        assert_type(await obj.method.asyncio(x), Any)  # TODO: int

        await non_generator.asyncio()  # type: ignore[call-arg]
        await generator.asyncio()  # type: ignore[call-arg]
        await generic.asyncio(1, 2)  # type: ignore[call-arg]
        await obj.method.asyncio(1)  # type: ignore[arg-type]


def test_async_call() -> None:
    def f(x: int) -> str:
        return str(x)

    async_call(f, 1)
    if TYPE_CHECKING:
        async_call(f, 1, task_cls=FutureBase)  # TODO: this should be an error


def test_amax(x: int = 1, y: int = 2) -> None:
    if TYPE_CHECKING:
        assert_type(amax(1, 2, key=len), Any)  # TODO: int
        assert_type(amax([1, 2], key=len), Any)  # TODO: int
