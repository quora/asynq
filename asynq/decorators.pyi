from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Generic,
    Mapping,
    Optional,
    TypeVar,
    Union,
    overload,
)

import qcore.decorators
from typing_extensions import ParamSpec

from . import async_task, futures

_P = ParamSpec("_P")
_T = TypeVar("_T")
_G = Generator[Any, Any, _T]  # Generator that returns _T
_Coroutine = Coroutine[Any, Any, _T]
_CoroutineFn = Callable[..., _Coroutine]

def lazy(fn: Callable[_P, _T]) -> Callable[_P, futures.FutureBase[_T]]: ...
def has_async_fn(fn: object) -> bool: ...
def is_pure_async_fn(fn: object) -> bool: ...
def is_async_fn(fn: object) -> bool: ...
def get_async_fn(
    fn: object, wrap_if_none: bool = ...
) -> Optional[Callable[..., futures.FutureBase[Any]]]: ...
def get_async_or_sync_fn(fn: object) -> Any: ...

class PureAsyncDecoratorBinder(qcore.decorators.DecoratorBinder):
    def is_pure_async_fn(self) -> bool: ...

class PureAsyncDecorator(qcore.decorators.DecoratorBase, Generic[_T, _P]):
    binder_cls = PureAsyncDecoratorBinder

    @overload
    def __init__(
        self,
        fn: Callable[_P, Any],  # TODO overloads for Generator[Any, Any, _T] and _T
        task_cls: Optional[type[futures.FutureBase]],
        kwargs: Mapping[str, Any] = ...,
        asyncio_fn: Optional[Callable[_P, Coroutine[Any, Any, _T]]] = ...,
    ) -> None: ...
    @overload
    def __init__(
        self,
        fn: Callable[OriginalFunctionParams, Generator[Any, Any, _T]],
        task_cls: Optional[Type[futures.FutureBase]],
        kwargs: Mapping[str, Any] = ...,
        asyncio_fn: Optional[Callable[_P, Coroutine[Any, Any, _T]]] = ...,
    ) -> None: ...
    def name(self) -> str: ...
    def is_pure_async_fn(self) -> bool: ...
    def asyncio(
        self, *args: _P.args, **kwargs: _P.kwargs
    ) -> Coroutine[Any, Any, _T]: ...
    def __call__(
        self, *args: Any, **kwargs: Any
    ) -> Union[_T, futures.FutureBase[_T]]: ...
    def __get__(self, owner: Any, cls: Any) -> PureAsyncDecorator[_T, _P]: ...  # type: ignore[override]

class AsyncDecoratorBinder(qcore.decorators.DecoratorBinder, Generic[_T]):
    def asynq(self, *args: Any, **kwargs: Any) -> async_task.AsyncTask[_T]: ...
    def asyncio(self, *args, **kwargs) -> Coroutine[Any, Any, _T]: ...

class AsyncDecorator(PureAsyncDecorator[_T, _P]):
    binder_cls = AsyncDecoratorBinder  # type: ignore
    @overload
    def __init__(
        self,
        fn: Callable[_P, Any],  # TODO overloads for Generator[Any, Any, _T] and _T
        cls: Optional[type[futures.FutureBase]],
        kwargs: Mapping[str, Any] = ...,
        asyncio_fn: Optional[Callable[_P, Coroutine[Any, Any, _T]]] = ...,
    ): ...
    @overload
    def __init__(
        self,
        fn: Callable[OriginalFunctionParams, Generator[Any, Any, _T]],
        cls: Optional[Type[futures.FutureBase]],
        kwargs: Mapping[str, Any] = ...,
        asyncio_fn: Optional[Callable[_P, Coroutine[Any, Any, _T]]] = ...,
    ): ...
    def is_pure_async_fn(self) -> bool: ...
    def asynq(self, *args: Any, **kwargs: Any) -> async_task.AsyncTask[_T]: ...
    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _T: ...
    def __get__(self, owner: Any, cls: Any) -> AsyncDecorator[_T, _P]: ...  # type: ignore[override]

class AsyncAndSyncPairDecoratorBinder(AsyncDecoratorBinder[_T]): ...

class AsyncAndSyncPairDecorator(AsyncDecorator[_T, _P]):
    binder_cls = AsyncAndSyncPairDecoratorBinder  # type: ignore
    def __init__(
        self,
        fn: Callable[..., futures.FutureBase[_T]],
        cls: Optional[type[futures.FutureBase]],
        sync_fn: Callable[..., _T],
        kwargs: Mapping[str, Any] = ...,
    ) -> None: ...
    def __call__(self, *args: Any, **kwargs: Any) -> _T: ...
    def __get__(self, owner: Any, cls: Any) -> Any: ...

class AsyncProxyDecorator(AsyncDecorator[_T, _P]):
    def __init__(
        self,
        fn: Callable[..., futures.FutureBase[_T]],
        asyncio_fn: Optional[Callable[..., Coroutine[Any, Any, _T]]] = ...,
    ) -> None: ...

class AsyncAndSyncPairProxyDecorator(AsyncProxyDecorator[_T, _P]):
    def __init__(
        self,
        fn: Callable[..., futures.FutureBase[_T]],
        sync_fn: Callable[..., _T],
        asyncio_fn: Optional[Callable[..., Coroutine[Any, Any, _T]]] = ...,
    ) -> None: ...
    def __call__(self, *args: Any, **kwargs: Any) -> _T: ...

class _MkAsyncDecorator:
    @overload
    def __call__(
        self, fn: Callable[_P, Generator[Any, Any, _T]]
    ) -> AsyncDecorator[_T, _P]: ...
    @overload
    def __call__(
        self, fn: Callable[_P, _T]
    ) -> AsyncDecorator[_T, _P]: ...

class _MkPureAsyncDecorator:
    @overload
    def __call__(
        self, fn: Callable[_P, Generator[Any, Any, _T]]
    ) -> PureAsyncDecorator[_T, _P]: ...
    @overload
    def __call__(
        self, fn: Callable[_P, _T]
    ) -> PureAsyncDecorator[_T, _P]: ...

# In reality these two can return other Decorator subclasses, but that doesn't matter for callers.
@overload
def asynq(  # type: ignore
    *,
    sync_fn: Optional[Callable[_P, _T]] = ...,
    cls: Type[futures.FutureBase] = ...,
    asyncio_fn: Optional[Callable[_P, Coroutine[Any, Any, _T]]] = ...,
    **kwargs: Any,
) -> _MkAsyncDecorator: ...
@overload
def asynq(  # type: ignore
    *,
    sync_fn: Optional[Callable[_P, Generator[Any, Any, _T]]] = ...,
    cls: Type[futures.FutureBase] = ...,
    asyncio_fn: Optional[Callable[_P, Coroutine[Any, Any, _T]]] = ...,
    **kwargs: Any,
) -> _MkAsyncDecorator: ...
@overload
def asynq(
    pure: bool,
    sync_fn: Optional[
        Callable[_P, Union[_T, Generator[Any, Any, _T]]]
    ] = ...,
    cls: Type[futures.FutureBase] = ...,
    asyncio_fn: Optional[Callable[_P, Coroutine[Any, Any, _T]]] = ...,
    **kwargs: Any,
) -> _MkPureAsyncDecorator: ...
@overload
def async_proxy(
    *,
    sync_fn: Optional[
        Callable[OriginalFunctionParams, Union[_T, Generator[Any, Any, _T]]]
    ] = ...,
    asyncio_fn: Optional[Callable[..., Coroutine[Any, Any, _T]]] = ...,
) -> _MkAsyncDecorator: ...
@overload
def async_proxy(
    pure: bool,
    sync_fn: Optional[
        Callable[OriginalFunctionParams, Union[_T, Generator[Any, Any, _T]]]
    ] = ...,
    asyncio_fn: Optional[Callable[..., Coroutine[Any, Any, _T]]] = ...,
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
