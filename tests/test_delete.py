"""
Unit tests for table deletion operations
"""

import pytest


def test_drop_table(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test dropping a table"""
    cleanup_table(test_table_name)
    
    # Create table
    ice_frame.create_table(test_table_name, sample_schema)
    assert ice_frame.table_exists(test_table_name)
    
    # Drop table
    ice_frame.drop_table(test_table_name)
    
    # Verify table is gone
    assert not ice_frame.table_exists(test_table_name)


def test_drop_nonexistent_table(ice_frame):
    """Test dropping a table that doesn't exist"""
    # Should raise an exception or handle gracefully
    with pytest.raises(Exception):
        ice_frame.drop_table("nonexistent_table_xyz_12345")


def test_delete_from_table(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test deleting rows from a table"""
    cleanup_table(test_table_name)
    
    # Create table and add data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Verify initial data
    df_before = ice_frame.read_table(test_table_name)
    initial_count = len(df_before)
    assert initial_count == len(sample_data)
    
    # Note: Delete functionality depends on PyIceberg version and catalog support
    # This test may need to be adjusted based on actual implementation
    try:
        # Try to delete rows where id < 3
        ice_frame.delete_from_table(test_table_name, "id < 3")
        
        # Read and verify
        df_after = ice_frame.read_table(test_table_name)
        # Should have fewer rows
        assert len(df_after) < initial_count
    except (NotImplementedError, AttributeError) as e:
        # Delete may not be supported in all PyIceberg versions
        pytest.skip(f"Delete operation not supported: {e}")
