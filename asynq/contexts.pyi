from abc import abstractmethod
from typing import Type, Optional, TypeVar
from types import TracebackType

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
