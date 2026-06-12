import shutil
import os
import polars as pl
from iceframe import IceFrame

# Setup
WAREHOUSE_PATH = "./test_warehouse_adv_gc"
if os.path.exists(WAREHOUSE_PATH):
    shutil.rmtree(WAREHOUSE_PATH)
os.makedirs(WAREHOUSE_PATH)

config = {
    "uri": f"sqlite:///{WAREHOUSE_PATH}/catalog.db",
    "type": "sql",
    "warehouse": f"file://{os.path.abspath(WAREHOUSE_PATH)}"
}

ice = IceFrame(config)

def test_smart_skipping():
    print("\n--- Testing Smart Skip Logic ---")
    
    # Create table partitioned by category
    df = pl.DataFrame({
        "id": [1],
        "category": ["A"],
        "data": ["x"]
    })
    ice.create_table("default.smart_test", df, partition_spec=[("category", "identity")])
    
    # Partition A: Create 5 files
    print("Creating 5 files in Partition A...")
    for _ in range(5):
        ice.append_to_table("default.smart_test", pl.DataFrame({"id": [1], "category": ["A"], "data": ["x"]}))
        
    # Partition B: Create 1 file
    print("Creating 1 file in Partition B...")
    ice.append_to_table("default.smart_test", pl.DataFrame({"id": [2], "category": ["B"], "data": ["y"]}))
    
    # Compact with min_input_files=3
    # Expect: Partition A compacted, Partition B skipped
    print("Compacting with min_input_files=3...")
    result = ice.compact_data_files("default.smart_test", target_file_size_mb=1, min_input_files=3)
    
    print("Result:", result)
    
    # Verification
    # To verify skipping vs compacting, strict file count check is best, 
    # but result stats should tell us skipped count.
    
    if result.get("skipped_partitions") != 1:
        raise AssertionError(f"Expected 1 skipped partition, got {result.get('skipped_partitions')}")
        
    # Also verify partition A was compacted (should be 1 file now effectively in that partition, or at least rewritten)
    # Total rows rewritten should be 5 (from partition A)
    # The 1 file from B shouldn't be touched.
    
    if result.get("rewritten_rows") != 5:
         raise AssertionError(f"Expected 5 rewritten rows, got {result.get('rewritten_rows')}")
         
    print("Smart Skipping Test Passed!")

if __name__ == "__main__":
    test_smart_skipping()
    print("\nADVANCED COMPACTION VERIFIED")
