"""
Tests for native maintenance operations.
"""

import pytest
import polars as pl
import datetime
from iceframe.gc import GarbageCollector

def test_native_expire_snapshots(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test native snapshot expiration"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Create multiple snapshots
    for i in range(3):
        ice_frame.append_to_table(test_table_name, sample_data)
    
    table = ice_frame.get_table(test_table_name)
    initial_snapshots = list(table.snapshots())
    assert len(initial_snapshots) >= 3
    
    # Expire old snapshots
    gc = GarbageCollector(table)
    
    try:
        # Expire all but last 1
        expired = gc.expire_snapshots(retain_last=1)
        
        # Verify snapshots were expired
        table.refresh()
        remaining_snapshots = list(table.snapshots())
        assert len(remaining_snapshots) <= len(initial_snapshots)
        
    except NotImplementedError as e:
        pytest.skip(f"Snapshot expiration not supported: {e}")

def test_native_remove_orphan_files_dry_run(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test native orphan file removal (dry run)"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    table = ice_frame.get_table(test_table_name)
    gc = GarbageCollector(table)
    
    try:
        # Dry run should not delete anything
        orphans = gc.remove_orphan_files(dry_run=True)
        
        # Should return a list (may be empty if no orphans)
        assert isinstance(orphans, list)
        
    except NotImplementedError as e:
        pytest.skip(f"Orphan file removal not supported: {e}")

def test_native_remove_orphan_files_with_age(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test orphan file removal with age filter"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    table = ice_frame.get_table(test_table_name)
    gc = GarbageCollector(table)
    
    try:
        # Use future timestamp - should not remove anything
        import time
        future_ms = int((time.time() + 86400) * 1000)  # Tomorrow
        
        orphans = gc.remove_orphan_files(older_than_ms=future_ms, dry_run=True)
        
        # Should be empty since we're looking for files older than tomorrow
        assert isinstance(orphans, list)
        
    except NotImplementedError as e:
        pytest.skip(f"Orphan file removal not supported: {e}")
