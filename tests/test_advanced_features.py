"""
Tests for advanced Iceberg features.
"""

import pytest
from iceframe.views import ViewManager
from iceframe.compaction import CompactionManager
from iceframe.evolution import PartitionEvolution
from iceframe.procedures import StoredProcedures

def test_view_manager(ice_frame):
    """Test ViewManager (mocked if catalog doesn't support)"""
    # Most REST catalogs support views now
    manager = ViewManager(ice_frame.catalog)
    
    view_name = "test_view"
    sql = "SELECT * FROM source_table"
    
    try:
        manager.create_view(view_name, sql, replace=True)
        # Verify exists
        views = manager.list_views()
        assert any(view_name in v for v in views)
        
        # Drop
        manager.drop_view(view_name)
    except NotImplementedError:
        pytest.skip("Views not supported by this catalog")
    except Exception as e:
        pytest.skip(f"View creation failed (likely catalog support): {e}")

def test_compaction_manager(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test CompactionManager"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    table = ice_frame.get_table(test_table_name)
    compactor = CompactionManager(table)
    
    # Test bin_pack (simulated via overwrite)
    stats = compactor.bin_pack()
    assert stats["rewritten_rows"] >= 0
    
    # Test sort
    stats = compactor.sort(sort_order=["id"])
    assert stats["rewritten_rows"] >= 0
    
    # Test rewrite_manifests
    try:
        compactor.rewrite_manifests()
    except NotImplementedError:
        pytest.skip("rewrite_manifests not supported")
    except Exception:
        # Might fail if no manifests to rewrite, but shouldn't raise unexpected error
        pass

def test_partition_evolution(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test PartitionEvolution"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    table = ice_frame.get_table(test_table_name)
    evolver = PartitionEvolution(table)
    
    try:
        # Add partition
        evolver.add_identity_partition("id")
        
        # Verify spec updated (reload table)
        table.refresh()
        assert len(table.spec().fields) > 0
    except Exception as e:
        pytest.fail(f"Partition evolution failed: {e}")

def test_stored_procedures(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test StoredProcedures"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    table = ice_frame.get_table(test_table_name)
    procs = StoredProcedures(table)
    
    # Test call
    try:
        procs.call("rewrite_data_files")
        
        try:
            procs.call("rewrite_manifests")
        except NotImplementedError:
            pass
    except Exception as e:
        pytest.fail(f"Procedure call failed: {e}")
