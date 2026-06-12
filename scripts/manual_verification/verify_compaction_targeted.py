import shutil
import os
import polars as pl
from iceframe import IceFrame

# Setup
WAREHOUSE_PATH = "./test_warehouse_targeted"
if os.path.exists(WAREHOUSE_PATH):
    shutil.rmtree(WAREHOUSE_PATH)
os.makedirs(WAREHOUSE_PATH)

config = {
    "uri": f"sqlite:///{WAREHOUSE_PATH}/catalog.db",
    "type": "sql",
    "warehouse": f"file://{os.path.abspath(WAREHOUSE_PATH)}"
}

ice = IceFrame(config)

def test_targeted_compaction():
    print("\n--- Setup Data ---")
    df = pl.DataFrame({
        "id": [1, 1], # Duplicate
        "category": ["A", "B"],
        "data": ["x", "y"]
    })
    ice.create_table("default.targeted_test", df, partition_spec=[("category", "identity")])
    ice.append_to_table("default.targeted_test", df) # More duplicates
    
    # Test 1: Partition Targeting
    print("\n--- Test 1: Compact Partition A Only ---")
    stats = ice.compact_data_files("default.targeted_test", partition_filter={"category": "A"})
    print("Result:", stats)
    
    # Should only process partition A (1 partition rewritten)
    if stats.get("rewritten_partitions") != 1:
        raise AssertionError("Expected exactly 1 partition rewritten")
        
    # Test 2: Deduplication
    print("\n--- Test 2: Deduplicate Partition B ---")
    # Partition B currently has 2 files with duplicate row: id=1, cat=B, data=y
    # Total rows in B before: 2. After dedup: 1.
    
    stats_dedup = ice.compact_data_files(
        "default.targeted_test", 
        partition_filter={"category": "B"},
        deduplicate=True
    )
    print("Result:", stats_dedup)
    
    if stats_dedup.get("rewritten_partitions") != 1:
         raise AssertionError("Expected Partition B rewritten")
         
    # Verify Data
    final_df = ice.read_table("default.targeted_test").filter(pl.col("category") == "B")
    print("Final Partition B Rows:", final_df.height)
    
    if final_df.height != 1:
        raise AssertionError(f"Expected 1 unique row in Partition B, got {final_df.height}")
        
    print("TARGETED COMPACTION VERIFIED")

if __name__ == "__main__":
    test_targeted_compaction()
