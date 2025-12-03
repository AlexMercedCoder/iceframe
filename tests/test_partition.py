"""
Unit tests for Partition Management
"""

import pytest
from iceframe.partition import PartitionManager

def test_add_drop_partition(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test adding and dropping partition fields"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add partition by 'name' (identity)
    ice_frame.partition_by(test_table_name).add_partition_field("name")
    
    # Verify
    table = ice_frame.get_table(test_table_name)
    assert len(table.spec().fields) == 1
    assert table.spec().fields[0].name == "name"
    
    # Add partition by 'created_at' (day)
    ice_frame.partition_by(test_table_name).add_partition_field("created_at", "day", name="created_day")
    
    # Verify
    table = ice_frame.get_table(test_table_name)
    assert len(table.spec().fields) == 2
    assert table.spec().fields[1].name == "created_day"
    
    # Drop partition 'name'
    ice_frame.partition_by(test_table_name).drop_partition_field("name")
    
    # Verify
    table = ice_frame.get_table(test_table_name)
    # Note: Dropping partition field in Iceberg v2 creates a new spec ID, 
    # but fields list in current spec should reflect removal (it becomes void transform or removed from list depending on implementation)
    # PyIceberg remove_field removes it from the list of fields in the new spec
    assert len(table.spec().fields) == 1
    assert table.spec().fields[0].name == "created_day"

def test_bucket_partition(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test bucket partition"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add bucket partition on 'id'
    ice_frame.partition_by(test_table_name).add_partition_field("id", "bucket", 16, name="id_bucket")
    
    # Verify
    table = ice_frame.get_table(test_table_name)
    assert table.spec().fields[0].name == "id_bucket"
    assert str(table.spec().fields[0].transform) == "bucket[16]"
