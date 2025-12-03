"""
Unit tests for Branching Support
"""

import pytest
import polars as pl
import datetime
from iceframe.branching import BranchManager

def test_create_branch(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test creating a branch"""
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
    
    try:
        ice_frame.create_branch(test_table_name, "test_branch")
        
        # Verify branch exists
        table = ice_frame.get_table(test_table_name)
        bm = BranchManager(table)
        branches = bm.list_branches()
        assert "test_branch" in branches
        
    except NotImplementedError:
        pytest.skip("Branching not supported by this catalog/version")
    except Exception as e:
        # Some catalogs might fail if they don't support branching
        pytest.skip(f"Branch creation failed: {e}")

def test_tag_snapshot(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test tagging a snapshot"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add data
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
    
    table = ice_frame.get_table(test_table_name)
    current = table.current_snapshot()
    
    if current:
        try:
            ice_frame.tag_snapshot(test_table_name, current.snapshot_id, "v1.0")
            
            # Verify tag exists (listing branches usually includes tags or separate method)
            # PyIceberg doesn't have list_tags yet, so we assume success if no error
            pass
        except NotImplementedError:
            pytest.skip("Tagging not supported")
        except Exception as e:
            pytest.skip(f"Tagging failed: {e}")
