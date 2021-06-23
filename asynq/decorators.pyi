import qcore.decorators
from typing import (
    Any,
    Callable,
    Generic,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
    overload,
)
from . import async_task
from . import futures

_T = TypeVar("_T")

def lazy(fn: Callable[..., _T]) -> Callable[..., futures.FutureBase[_T]]: ...
def has_async_fn(fn: object) -> bool: ...
def is_pure_async_fn(fn: object) -> bool: ...
def is_async_fn(fn: object) -> bool: ...
def get_async_fn(
    fn: object, wrap_if_none: bool = ...
) -> Optional[Callable[..., futures.FutureBase[Any]]]: ...
def get_async_or_sync_fn(fn: object) -> Any: ...

class PureAsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    def is_pure_async_fn(self) -> bool: ...

class PureAsyncDecorator(qcore.decorators.DecoratorBase, Generic[_T]):
    binder_cls = PureAsyncDecoratorBinder
    def __init__(
        self,
        fn: Callable[..., _T],
        task_cls: Optional[Type[futures.FutureBase]],
        kwargs: Mapping[str, Any] = ...,
    ) -> None: ...
    def name(self) -> str: ...
    def is_pure_async_fn(self) -> bool: ...
    def __call__(
        self, *args: Any, **kwargs: Any
    ) -> Union[_T, futures.FutureBase[_T]]: ...
    def __get__(self, owner: Any, cls: Any) -> PureAsyncDecorator[_T]: ...  # type: ignore

class AsyncDecoratorBinder(qcore.decorators.DecoratorBinder, Generic[_T]):
    def asynq(self, *args: Any, **kwargs: Any) -> async_task.AsyncTask[_T]: ...

class AsyncDecorator(PureAsyncDecorator[_T]):
    binder_cls = AsyncDecoratorBinder  # type: ignore
    def is_pure_async_fn(self) -> bool: ...
    def asynq(self, *args: Any, **kwargs: Any) -> async_task.AsyncTask[_T]: ...
    def __call__(self, *args: Any, **kwargs: Any) -> _T: ...
    def __get__(self, owner: Any, cls: Any) -> AsyncDecorator[_T]: ...  # type: ignore

class AsyncAndSyncPairDecoratorBinder(AsyncDecoratorBinder[_T]): ...

class AsyncAndSyncPairDecorator(AsyncDecorator[_T]):
    binder_cls = AsyncAndSyncPairDecoratorBinder  # type: ignore
    def __init__(
        self,
        fn: Callable[..., futures.FutureBase[_T]],
        cls: Optional[Type[futures.FutureBase]],
        sync_fn: Callable[..., _T],
        kwargs: Mapping[str, Any] = ...,
    ) -> None: ...
    def __call__(self, *args: Any, **kwargs: Any) -> _T: ...
    def __get__(self, owner: Any, cls: Any) -> Any: ...

class AsyncProxyDecorator(AsyncDecorator[_T]):
    def __init__(self, fn: Callable[..., futures.FutureBase[_T]]) -> None: ...

class AsyncAndSyncPairProxyDecorator(AsyncProxyDecorator[_T]):
    def __init__(
        self, fn: Callable[..., futures.FutureBase[_T]], sync_fn: Callable[..., _T]
    ) -> None: ...
    def __call__(self, *args: Any, **kwargs: Any) -> _T: ...

class _MkAsyncDecorator:
    def __call__(self, fn: Callable[..., _T]) -> AsyncDecorator[_T]: ...

class _MkPureAsyncDecorator:
    def __call__(self, fn: Callable[..., _T]) -> PureAsyncDecorator[_T]: ...

# In reality these two can return other Decorator subclasses, but that doesn't matter for callers.
@overload
def asynq(  # type: ignore
    *,
    sync_fn: Optional[Callable[..., Any]] = ...,
    cls: Type[futures.FutureBase] = ...,
    **kwargs: Any,
) -> _MkAsyncDecorator: ...
@overload
def asynq(
    pure: bool,
    sync_fn: Optional[Callable[..., Any]] = ...,
    cls: Type[futures.FutureBase] = ...,
    **kwargs: Any,
) -> _MkPureAsyncDecorator: ...  # type: ignore
@overload
def async_proxy(
    *, sync_fn: Optional[Callable[..., Any]] = ...
) -> _MkAsyncDecorator: ...
@overload
def async_proxy(
    pure: bool, sync_fn: Optional[Callable[..., Any]] = ...
) -> _MkPureAsyncDecorator: ...
@asynq()
def async_call(
    fn: Union[Callable[..., _T], Callable[..., futures.FutureBase[_T]]],
    *args: Any,
    **kwargs: Any,
) -> _T: ...
def make_async_decorator(
    fn: Callable[..., Any],
    wrapper_fn: Callable[..., futures.FutureBase[Any]],
    name: str,
) -> Any: ...
