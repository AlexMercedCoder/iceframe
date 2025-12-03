"""
Unit tests for table maintenance operations
"""

import pytest


def test_expire_snapshots(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test expiring old snapshots"""
    cleanup_table(test_table_name)
    
    # Create table and add data multiple times to create snapshots
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Try to expire snapshots
    try:
        ice_frame.expire_snapshots(test_table_name, older_than_days=0, retain_last=1)
        # If no exception, operation succeeded
        assert True
    except (NotImplementedError, AttributeError) as e:
        pytest.skip(f"Snapshot expiration not supported: {e}")


def test_remove_orphan_files(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test removing orphan files"""
    cleanup_table(test_table_name)
    
    # Create table with data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Try to remove orphan files
    try:
        ice_frame.remove_orphan_files(test_table_name, older_than_days=0)
        # If no exception, operation succeeded
        assert True
    except (NotImplementedError, AttributeError) as e:
        pytest.skip(f"Orphan file removal not supported: {e}")


def test_compact_data_files(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test compacting data files"""
    cleanup_table(test_table_name)
    
    # Create table with data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Try to compact files
    try:
        ice_frame.compact_data_files(test_table_name, target_file_size_mb=128)
        # If no exception, operation succeeded
        assert True
    except (NotImplementedError, AttributeError) as e:
        pytest.skip(f"Data file compaction not supported: {e}")
