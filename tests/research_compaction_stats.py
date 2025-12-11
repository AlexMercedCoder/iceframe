import shutil
import os
import polars as pl
from iceframe import IceFrame
from pyiceberg.table import Table

# Setup
WAREHOUSE_PATH = "./test_warehouse_research"
if os.path.exists(WAREHOUSE_PATH):
    shutil.rmtree(WAREHOUSE_PATH)
os.makedirs(WAREHOUSE_PATH)

config = {
    "uri": f"sqlite:///{WAREHOUSE_PATH}/catalog.db",
    "type": "sql",
    "warehouse": f"file://{os.path.abspath(WAREHOUSE_PATH)}"
}

ice = IceFrame(config)

def research_stats():
    # Create simple partitioned table
    df = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "category": ["A", "A", "B", "B"],
        "data": ["x", "y", "z", "w"]
    })
    ice.create_table("default.stats_test", df, partition_spec=[("category", "identity")])
    
    # Create multiple files per partition
    ice.append_to_table("default.stats_test", df) # 2 files (1 per partition)
    ice.append_to_table("default.stats_test", df) # +2 files
    
    table = ice.get_table("default.stats_test")
    
    print("\n--- Inspecting File Scan Tasks ---")
    tasks = list(table.scan().plan_files())
    
    partition_stats = {}
    
    for task in tasks:
        # Task has .file which is DataFile
        f = task.file
        p = f.partition # Struct
        size = f.file_size_in_bytes
        path = f.file_path
        
        # Determine partition key (tuple representation)
        # Partition record might be complex, let's see how to represent it
        # For identity, it's straight forward
        print(f"File: {path}, Size: {size}, Partition: {p}")
        
        # Group by partition to calculate metrics
        # We need a hashable key for partition
        # p is a Record, usually hashable?
        # Let's try str(p) for now
        p_key = str(p)
        
        if p_key not in partition_stats:
            partition_stats[p_key] = {"count": 0, "bytes": 0, "files": []}
            
        partition_stats[p_key]["count"] += 1
        partition_stats[p_key]["bytes"] += size
        partition_stats[p_key]["files"].append(path)
        
    print("\n--- Partition Stats ---")
    for p, stats in partition_stats.items():
        avg_size = stats["bytes"] / stats["count"]
        print(f"Partition {p}: {stats['count']} files, {stats['bytes']} bytes, Avg: {avg_size:.0f} bytes")
        
        # Decision logic research
        TARGET_SIZE = 1000 # dummy small target
        if stats["count"] > 1 and avg_size < TARGET_SIZE:
             print(" -> Needs Compaction")
        else:
             print(" -> Healthy")

if __name__ == "__main__":
    research_stats()
