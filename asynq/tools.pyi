from .contexts import AsyncContext
from asynq import asynq, async_proxy

from qcore import Utime
from qcore.events import EventHook
from typing import (
    Any,
    Callable,
    ContextManager,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    overload,
)

_T = TypeVar("_T")
_U = TypeVar("_U")

@asynq()
def amap(function: Callable[[_T], _U], sequence: Iterable[_T]) -> List[_U]: ...
@asynq()
def afilter(
    function: Optional[Callable[[_T], bool]], sequence: Iterable[_T]
) -> List[_T]: ...
@asynq()
def afilterfalse(
    function: Callable[[_T], bool], sequence: Iterable[_T]
) -> List[_T]: ...
@asynq()
def asorted(
    iterable: Iterable[_T],
    key: Optional[Callable[[_T], Any]] = ...,
    reverse: bool = ...,
) -> List[_T]: ...
@overload
@asynq()
def amax(__arg: Iterable[_T], key: Optional[Callable[[_T], Any]] = ...) -> _T: ...
@overload
@asynq()
def amax(*args: _T, key: Optional[Callable[[_T], Any]] = ...) -> _T: ...
@overload
@asynq()
def amin(__arg: Iterable[_T], key: Optional[Callable[[_T], Any]] = ...) -> _T: ...
@overload
@asynq()
def amin(*args: _T, key: Optional[Callable[[_T], Any]] = ...) -> _T: ...
@asynq()
def asift(
    pred: Callable[[_T], bool], items: Iterable[_T]
) -> Tuple[List[_T], List[_T]]: ...

# Used to implement decorators that just wrap a function without changing the signature.
class _AsyncWrapper:
    def __call__(self, fn: _T) -> _T: ...

def acached_per_instance() -> _AsyncWrapper: ...
def alru_cache(
    maxsize: int = ..., key_fn: Optional[Callable[..., Any]] = ...
) -> _AsyncWrapper: ...
def alazy_constant(ttl: int = ...) -> Callable[..., Any]: ...
def aretry(
    exception_cls: type, max_tries: int = ..., sleep: float = ...
) -> Callable[..., Any]: ...
@asynq()
def call_with_context(
    context: ContextManager[Any], fn: Callable[..., _T], *args: Any, **kwargs: Any
) -> _T: ...
def deduplicate(keygetter: Optional[Callable[..., Any]] = ...) -> _AsyncWrapper: ...

class AsyncTimer(AsyncContext):
    total_time: Utime
    def __init__(self) -> None: ...
    def resume(self) -> None: ...
    def pause(self) -> None: ...

class AsyncEventHook(EventHook):
    @asynq()
    def trigger(self, *args: Any) -> None: ...  # type: ignore
    @asynq()
    def safe_trigger(self, *args: Any) -> None: ...  # type: ignore
