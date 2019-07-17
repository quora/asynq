from abc import abstractmethod
from typing import ContextManager, Type, Optional, TypeVar
from types import TracebackType
import asynq

from .futures import FutureBase

_T = TypeVar("_T", bound=AsyncContext)

class NonAsyncContext(object):
    def __enter__(self) -> None: ...
    def __exit__(
        self,
        typ: Optional[Type[BaseException]],
        val: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None: ...
    def pause(self) -> None: ...
    def resume(self) -> None: ...

def enter_context(context: ContextManager[object]) -> FutureBase: ...
def leave_context(context: ContextManager[object], active_task: FutureBase) -> None: ...

class AsyncContext(object):
    def __enter__(self: _T) -> _T: ...
    def __exit__(
        self,
        typ: Optional[Type[BaseException]],
        val: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None: ...
    @abstractmethod
    def resume(self) -> None: ...
    @abstractmethod
    def pause(self) -> None: ...
