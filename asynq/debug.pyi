from builtins import str as _str
import contextlib
import logging
from types import FrameType, GeneratorType, TracebackType
from typing import Any, ContextManager, Iterator, List, Optional, Tuple, Type, overload, Union

from ._debug import options as options
@overload
def DUMP_ALL(self, value: None = ...) -> bool: ...
@overload
def DUMP_ALL(self, value: bool) -> None: ...
def enable_original_exc_handler(enable: bool) -> None: ...
def enable_filter_traceback(enable: bool) -> None: ...
def enable_traceback_syntax_highlight(enable: bool) -> None: ...
def dump_error(error: BaseException, tb: Optional[TracebackType] = ...) -> None: ...
def format_error(error: BaseException, tb: Optional[TracebackType] = ...) -> _str: ...

class AsynqStackTracebackFormatter(logging.Formatter):
    def formatException(
        self, exc_info: Union[Tuple[type, BaseException, Optional[TracebackType]], Tuple[None, None, None]]
    ) -> _str: ...

def extract_tb(
    tb: TracebackType, limit: Optional[int] = ...
) -> List[Tuple[_str, int, _str, _str]]: ...
def format_tb(tb: TracebackType) -> _str: ...
def dump_stack(skip: int = ..., limit: Optional[int] = ...) -> None: ...
def dump_asynq_stack() -> None: ...
def format_asynq_stack() -> Optional[List[_str]]: ...
def dump(state: Any) -> None: ...
def write(text: _str, indent: int = ...) -> None: ...
def str(source: object, truncate: bool = ...) -> _str: ...
def repr(source: object, truncate: bool = ...) -> _str: ...
def async_exception_hook(
    type: Type[BaseException], error: BaseException, tb: TracebackType
) -> None: ...
def ipython_custom_exception_handler(
    self: Any,
    etype: Type[BaseException],
    value: BaseException,
    tb: TracebackType,
    tb_offset: Any = ...,
) -> None: ...
def attach_exception_hook() -> None: ...
def detach_exception_hook() -> None: ...
@contextlib.contextmanager
def enable_complex_assertions(enable: bool = ...) -> Iterator[None]: ...
def disable_complex_assertions() -> ContextManager[None]: ...
def sync() -> None: ...
def get_frame(generator: GeneratorType) -> Optional[FrameType]: ...
def filter_traceback(tb_list: List[_str]) -> List[_str]: ...
def syntax_highlight_tb(_str) -> _str: ...

