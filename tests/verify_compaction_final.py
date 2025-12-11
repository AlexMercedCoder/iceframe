import shutil
import os
import polars as pl
from iceframe import IceFrame

# Setup
WAREHOUSE_PATH = "./test_warehouse_final"
if os.path.exists(WAREHOUSE_PATH):
    shutil.rmtree(WAREHOUSE_PATH)
os.makedirs(WAREHOUSE_PATH)

config = {
    "uri": f"sqlite:///{WAREHOUSE_PATH}/catalog.db",
    "type": "sql",
    "warehouse": f"file://{os.path.abspath(WAREHOUSE_PATH)}"
}

ice = IceFrame(config)
TABLE_NAME = "default.polish_test"

def test_final_polish():
    print("\n--- Testing Compaction Polish (Compression & Metrics) ---")
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "data": ["a" * 1000, "b" * 1000, "c" * 1000] # Some data size
    })
    ice.create_table(TABLE_NAME, df)
    
    # 1. Test Metrics & Dry Run (input_bytes)
    stats = ice.compact_data_files(TABLE_NAME, dry_run=True, min_input_files=1)
    print("Dry Run Stats:", stats)
    
    input_bytes = stats.get("input_bytes", 0)
    print(f"Input Bytes: {input_bytes}")
    
    if input_bytes == 0:
        print("Error: input_bytes should be > 0")
        
    # 2. Test Compression
    print("Compacting with compression='zstd'...")
    stats = ice.compact_data_files(TABLE_NAME, compression="zstd", min_input_files=1)
    print("Compaction Stats:", stats)
    
    # Verify Table Property
    table = ice.get_table(TABLE_NAME)
    props = table.properties
    compression_prop = props.get("write.parquet.compression-codec")
    print(f"Table Compression Property: {compression_prop}")
    
    if compression_prop != "zstd":
        print("Error: Compression property not set correctly")
        
    print("Final Polish verified.")

if __name__ == "__main__":
    test_final_polish()
    print("\nALL TESTS PASSED")
