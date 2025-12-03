"""
Unit tests for Branching Support
"""

import pytest
import polars as pl
import datetime

def test_branching_not_implemented(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test that branching raises NotImplementedError (requires newer PyIceberg)"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add data to create a snapshot
    data = pl.DataFrame({
        "id": [1],
        "name": ["A"],
        "age": [20],
        "created_at": [datetime.datetime.now()]
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(test_table_name, data)
    
    # Branching requires PyIceberg 0.6.0+ and catalog support
    with pytest.raises(NotImplementedError):
        ice_frame.create_branch(test_table_name, "test_branch")
        
def test_tagging_not_implemented(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test that tagging raises NotImplementedError (requires newer PyIceberg)"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    table = ice_frame.get_table(test_table_name)
    current = table.current_snapshot()
    
    if current:
        with pytest.raises(NotImplementedError):
            ice_frame.tag_snapshot(test_table_name, current.snapshot_id, "v1.0")
