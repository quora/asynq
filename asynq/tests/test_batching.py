# Copyright 2016 Quora, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from asynq import asynq, BatchBase, BatchItemBase
from .debug_cache import reset_caches, mc
from .caching import ExternalCacheBatchItem


def test_chain():
    @asynq()
    def foo(num_yield):
        if num_yield == 0:
            return 0

        yield ExternalCacheBatchItem(mc._batch, "get", "test")
        yield foo.asynq(num_yield - 1)

    reset_caches()
    foo(10)
    assert mc._batch.index == 11


def test_tree():
    @asynq()
    def foo(depth):
        if depth == 0:
            return (yield ExternalCacheBatchItem(mc._batch, "get", "test"))
        yield foo.asynq(depth - 1), foo.asynq(depth - 1)

    reset_caches()
    foo(5)
    assert mc._batch.index == 2


def test_batch_max_size():
    """Test that BatchBase respects max_batch_size parameter."""
    # Test basic functionality without size limit
    batch = BatchBase()
    assert batch.max_batch_size is None
    assert not batch.is_full()
    
    # Test with size limit
    batch_with_limit = BatchBase(max_batch_size=3)
    assert batch_with_limit.max_batch_size == 3
    assert not batch_with_limit.is_full()
    
    # Test is_full functionality
    class MockBatchItem(BatchItemBase):
        def __init__(self, batch):
            super().__init__(batch)
    
    # Add items one by one and check is_full status
    MockBatchItem(batch_with_limit)
    assert not batch_with_limit.is_full()
    assert len(batch_with_limit.items) == 1
    
    MockBatchItem(batch_with_limit)
    assert not batch_with_limit.is_full()
    assert len(batch_with_limit.items) == 2
    
    MockBatchItem(batch_with_limit)
    assert batch_with_limit.is_full()
    assert len(batch_with_limit.items) == 3


def test_batch_auto_switching():
    """Test automatic batch switching when max size is reached."""
    
    class TestBatch(BatchBase):
        switched_count = 0
        current_batch = None
        
        def __init__(self, max_batch_size=None):
            super().__init__(max_batch_size=max_batch_size)
            TestBatch.current_batch = self
        
        def _try_switch_active_batch(self):
            TestBatch.switched_count += 1
            TestBatch.current_batch = TestBatch(max_batch_size=self.max_batch_size)
        
        def _flush(self):
            # Mock implementation
            for item in self.items:
                item.set_value(f"result_{item.index}")
    
    class TestBatchItem(BatchItemBase):
        def __init__(self, max_batch_size=None):
            # Get or create current batch
            if TestBatch.current_batch is None or TestBatch.current_batch.is_full():
                if TestBatch.current_batch is not None:
                    TestBatch.current_batch._try_switch_active_batch()
            super().__init__(TestBatch.current_batch)
    
    # Reset state
    TestBatch.switched_count = 0
    TestBatch.current_batch = TestBatch(max_batch_size=2)
    
    # Create items that should trigger batch switching
    item1 = TestBatchItem()
    assert TestBatch.switched_count == 0
    assert len(TestBatch.current_batch.items) == 1
    
    item2 = TestBatchItem()
    assert TestBatch.switched_count == 0
    assert len(TestBatch.current_batch.items) == 2
    assert TestBatch.current_batch.is_full()
    
    # This should trigger batch switching
    item3 = TestBatchItem()
    assert TestBatch.switched_count == 1
    assert len(TestBatch.current_batch.items) == 1  # New batch with one item


def test_batch_empty_check():
    """Test that is_empty works correctly."""
    batch = BatchBase()
    assert batch.is_empty()
    
    class MockBatchItem(BatchItemBase):
        def __init__(self, batch):
            super().__init__(batch)
    
    MockBatchItem(batch)
    assert not batch.is_empty()
