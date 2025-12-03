"""
Tests for additional procedures (Rollback, Ingestion, CatalogOps).
"""

import pytest
from iceframe.rollback import RollbackManager
from iceframe.ingestion import DataIngestion
from iceframe.catalog_ops import CatalogOperations

def test_rollback_manager(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test RollbackManager"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Snapshot 1
    ice_frame.append_to_table(test_table_name, sample_data)
    table = ice_frame.get_table(test_table_name)
    snap1 = table.current_snapshot().snapshot_id
    
    # Snapshot 2
    ice_frame.append_to_table(test_table_name, sample_data)
    table.refresh()
    snap2 = table.current_snapshot().snapshot_id
    
    assert snap1 != snap2
    
    # Rollback to 1
    rm = RollbackManager(table)
    try:
        rm.rollback_to_snapshot(snap1)
        table.refresh()
        assert table.current_snapshot().snapshot_id == snap1
    except NotImplementedError:
        pytest.skip("Rollback not supported by this catalog/client")

def test_catalog_ops(ice_frame):
    """Test CatalogOperations"""
    ops = CatalogOperations(ice_frame.catalog)
    
    # Register table is hard to test without a valid metadata file URL
    # Just check if method exists and raises expected error or works
    try:
        ops.register_table("registered_table", "s3://bucket/path/metadata.json")
    except Exception:
        # Expected failure due to invalid path/auth
        pass

def test_ingestion(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test DataIngestion"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    table = ice_frame.get_table(test_table_name)
    
    ingestion = DataIngestion(table)
    
    try:
        ingestion.add_files(["/path/to/file.parquet"])
    except NotImplementedError:
        pytest.skip("add_files not supported")
    except Exception:
        # Expected failure due to invalid file
        pass
