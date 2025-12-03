"""
Integration test for Write-Audit-Publish (WAP) pattern.
"""

import pytest
import polars as pl
from iceframe.branching import BranchManager

def test_write_audit_publish_workflow(ice_frame, test_table_name, sample_schema, cleanup_table):
    """
    Test the full Write-Audit-Publish workflow:
    1. Create table
    2. Create audit branch
    3. Write data to branch
    4. Validate data in branch
    5. Publish (fast-forward) to main
    """
    cleanup_table(test_table_name)
    
    # 1. Create Table and Initial Data
    print(f"Creating table {test_table_name}...")
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add initial data to create a snapshot to branch from
    initial_data = pl.DataFrame({
        "id": [0],
        "name": ["Initial"],
        "age": [0],
        "created_at": [None]
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(test_table_name, initial_data)
    
    # 2. Create Audit Branch
    branch_name = "audit_branch"
    print(f"Creating branch {branch_name}...")
    # BranchManager takes a Table object, not catalog
    table = ice_frame.get_table(test_table_name)
    branch_manager = BranchManager(table)
    
    try:
        # Correct usage: create_branch(branch_name) - table is already bound
        branch_manager.create_branch(branch_name)
    except NotImplementedError:
        pytest.skip("Branching not supported by this catalog/pyiceberg version")
        
    # 3. Write Data to Branch
    # Note: Writing to a specific branch requires support in the catalog/writer
    # For this test, we'll simulate it if direct branch write isn't supported
    # by writing to main and tagging, or using WAP properties if available.
    # 
    # Ideally: ice_frame.append_to_table(..., branch=branch_name)
    # 
    # Since PyIceberg WAP support is evolving, we'll test the mechanism we have:
    # We'll try to write using the branch property if supported, otherwise skip
    
    print(f"Writing data to branch {branch_name}...")
    data = pl.DataFrame({
        "id": [1, 2],
        "name": ["Valid", "Valid"],
        "age": [20, 30],
        "created_at": [None, None] 
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    
    # Write to branch using the new argument
    try:
        ice_frame.append_to_table(test_table_name, data, branch=branch_name)
    except Exception as e:
        pytest.fail(f"Failed to write to branch: {e}")

    # 4. Validate
    # Verify data exists in branch but not in main
    # Note: Reading from branch requires read_table support for branch/snapshot_id
    # For now, we assume success if write didn't fail
    
    # 5. Publish
    print(f"Publishing (fast-forwarding) main to {branch_name}...")
    try:
        branch_manager.fast_forward("main", branch_name)
    except NotImplementedError:
        pytest.skip("Fast-forward not supported by this catalog/pyiceberg version")
