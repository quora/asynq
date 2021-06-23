import random
import threading
import time
import collections

from qcore import SECOND
from qcore.asserts import assert_eq, assert_le, assert_ge, assert_unordered_list_eq
from asynq import asynq, debug, profiler
from .debug_cache import reset_caches, mc
from .caching import ExternalCacheBatchItem


class ProfilerThread(threading.Thread):
    def __init__(self, stats_list):
        super().__init__()
        self.stats_list = stats_list[:]

    def run(self):
        profiler.reset()
        for stats in self.stats_list:
            profiler.append(stats)
        self.profiled_result = profiler.flush()


def test_multithreaded_profiler():
    """Test multithreaded profiler."""
    stats_list = list(range(10000))

    profiler.reset()
    assert_eq([], profiler.flush())

    threads = []
    for i in range(30):
        random.shuffle(stats_list)
        thread = ProfilerThread(stats_list)
        threads.append(thread)

    random.shuffle(stats_list)
    for stats in stats_list:
        profiler.append(stats)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
        assert_eq(thread.stats_list, thread.profiled_result)

    assert_eq(stats_list, profiler.flush())


def test_collect_perf_stats():
    sleep_time = 50000  # 50ms

    @asynq()
    def func(depth):
        if depth == 0:
            return (yield ExternalCacheBatchItem(mc._batch, "get", "test"))
        time.sleep(float(sleep_time) / SECOND)
        yield func.asynq(depth - 1), func.asynq(depth - 1)

    debug.options.COLLECT_PERF_STATS = True
    debug.options.KEEP_DEPENDENCIES = True
    profiler.reset()
    reset_caches()
    depth = 3
    func(depth)
    profiled_result = profiler.flush()

    batches = 1
    async_tasks = 2 ** (depth + 1) - 1
    num_leaf_tasks = 2 ** depth

    assert_eq(async_tasks + batches, len(profiled_result))

    deps_to_tasks = collections.defaultdict(list)
    for task in profiled_result:
        num_deps = len(task["dependencies"])
        deps_to_tasks[num_deps].append(task)

    assert_unordered_list_eq([1, 2, num_leaf_tasks], deps_to_tasks.keys())

    # leaf tasks
    assert_eq(num_leaf_tasks, len(deps_to_tasks[1]))
    for task in deps_to_tasks[1]:
        assert_ge(sleep_time, task["time_taken"])

    # one batch (with all the batch items as dependencies)
    assert_eq(1, len(deps_to_tasks[num_leaf_tasks]))

    # non-leaf async tasks in the tree (1 + 2 + 3 = 7)
    assert_eq(7, len(deps_to_tasks[2]))
    for task in deps_to_tasks[2]:
        assert_le(sleep_time, task["time_taken"])

    # When COLLET_PERF_STATS is False, we shouldn't profile anything.
    debug.options.COLLECT_PERF_STATS = False
    profiler.reset()
    reset_caches()
    func(2)
    profiled_result = profiler.flush()
    assert_eq(0, len(profiled_result))
