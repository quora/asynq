import random
import threading
import time

from qcore import SECOND
from qcore.asserts import assert_eq, assert_le, assert_ge
from asynq import asynq, debug, profiler, result
from .debug_cache import reset_caches, mc
from .caching import ExternalCacheBatchItem


class ProfilerThread(threading.Thread):
    def __init__(self, stats_list):
        threading.Thread.__init__(self)
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
            result((yield ExternalCacheBatchItem(mc._batch, 'get', 'test'))); return
        time.sleep(float(sleep_time) / SECOND)
        yield func.asynq(depth - 1), func.asynq(depth - 1)


    debug.options.COLLECT_PERF_STATS = True
    profiler.reset()
    reset_caches()
    depth = 3
    func(depth)
    profiled_result = profiler.flush()
    assert_eq(2 ** (depth + 1) - 1, len(profiled_result))
    for stats in profiled_result:
        num_deps = stats['num_deps']
        num_dependencies = len(stats['dependencies'])
        time_taken = stats['time_taken']

        # It has one ExternalCacheBatchItem, and no dependent AsyncTasks.
        if num_deps == 1:
            assert_eq(0, num_dependencies)
            assert_ge(sleep_time, time_taken)

        # It has two dependent AsyncTasks, and is sleeping for sleep_time (50ms).
        elif num_deps == 2:
            assert_eq(2, num_dependencies)
            assert_le(sleep_time, time_taken)
            assert_ge(sleep_time * 2, time_taken)
        else:
            assert False

    # When COLLET_PERF_STATS is False, we shouldn't profile anything.
    debug.options.COLLECT_PERF_STATS = False
    profiler.reset()
    reset_caches()
    func(2)
    profiled_result = profiler.flush()
    assert_eq(0, len(profiled_result))
