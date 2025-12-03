"""
Unit tests for Table Statistics
"""

import pytest
import polars as pl
import datetime

def test_get_stats(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test getting table statistics"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add some data
    data = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "age": [20, 30, 40],
        "created_at": [datetime.datetime.now()] * 3
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(test_table_name, data)
    
    # Get stats
    stats = ice_frame.stats(test_table_name)
    
    assert "table_name" in stats
    assert "schema" in stats
    assert stats["schema"]["fields"] == 4
    assert "snapshots" in stats
    assert stats["snapshots"]["count"] >= 1

def test_profile_column_numeric(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test profiling a numeric column"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add data
    data = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["A", "B", "C", "D", "E"],
        "age": [20, 30, 40, 50, 60],
        "created_at": [datetime.datetime.now()] * 5
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(test_table_name, data)
    
    # Profile age column
    profile = ice_frame.profile_column(test_table_name, "age")
    
    assert profile["column_name"] == "age"
    assert profile["null_count"] == 0
    assert profile["total_count"] == 5
    assert "numeric_stats" in profile
    assert profile["numeric_stats"]["min"] == 20
    assert profile["numeric_stats"]["max"] == 60
    assert profile["numeric_stats"]["mean"] == 40.0

def test_profile_column_string(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test profiling a string column"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add data
    data = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "BB", "CCC"],
        "age": [20, 30, 40],
        "created_at": [datetime.datetime.now()] * 3
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(test_table_name, data)
    
    # Profile name column
    profile = ice_frame.profile_column(test_table_name, "name")
    
    assert profile["column_name"] == "name"
    assert "string_stats" in profile
    assert profile["string_stats"]["min_length"] == 1
    assert profile["string_stats"]["max_length"] == 3
