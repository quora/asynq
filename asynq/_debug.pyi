from typing import overload

class DebugOptions(object):
    DUMP_PRE_ERROR_STATE: bool
    DUMP_EXCEPTIONS: bool
    DUMP_SCHEDULE_TASK: bool
    DUMP_CONTINUE_TASK: bool
    DUMP_SCHEDULE_BATCH: bool
    DUMP_FLUSH_BATCH: bool
    DUMP_DEPENDENCIES: bool
    DUMP_COMPUTED: bool
    DUMP_NEW_TASKS: bool
    DUMP_YIELD_RESULTS: bool
    DUMP_QUEUED_RESULTS: bool
    DUMP_CONTEXTS: bool
    DUMP_SYNC: bool
    DUMP_STACK: bool
    DUMP_SCHEDULER_STATE: bool
    DUMP_SYNC_CALLS: bool
    COLLECT_PERF_STATS: bool

    SCHEDULER_STATE_DUMP_INTERVAL: int
    DEBUG_STR_REPR_MAX_LENGTH: int
    STACK_DUMP_LIMIT: int

    ENABLE_COMPLEX_ASSERTIONS: bool
    def __init__(self) -> None: ...
    @overload
    def DUMP_ALL(self, value: None = ...) -> bool: ...
    @overload
    def DUMP_ALL(self, value: bool) -> None: ...

options: DebugOptions
