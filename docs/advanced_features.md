# Advanced Iceberg Features

IceFrame provides a suite of advanced features to give you a complete Iceberg experience, bridging gaps in the underlying libraries.

## Iceberg Views

Manage cross-engine views (if supported by your catalog).

```python
# Create a view
sql = "SELECT * FROM source_table WHERE id > 100"
ice.create_view("my_view", sql, replace=True)

# Drop a view
ice.drop_view("my_view")
```

## Advanced Compaction

Optimize your data layout for better query performance.

```python
from iceframe.compaction import CompactionManager

table = ice.get_table("my_table")
compactor = CompactionManager(table)

# Bin-pack small files into 128MB files
stats = compactor.bin_pack(target_file_size_mb=128)

# Sort data (Z-order approximation)
stats = compactor.sort(sort_order=["region", "ts"])
```

## Partition Evolution

Evolve your table partitioning without rewriting data.

```python
# Get evolution helper
evolver = ice.evolve_partition("my_table")

# Add partitions
evolver.add_day_partition("created_at")
evolver.add_bucket_partition("user_id", num_buckets=16)

# Remove partition
evolver.remove_partition("region")
```

## Stored Procedures

Execute maintenance tasks using a familiar procedure call interface.

```python
# Rewrite data files (compaction)
ice.call_procedure("my_table", "rewrite_data_files", target_file_size_mb=256)

# Expire snapshots (cleanup)
ice.call_procedure("my_table", "expire_snapshots", older_than_ms=...)

# Remove orphan files (GC)
ice.call_procedure("my_table", "remove_orphan_files")

# Fast-forward branch (WAP)
ice.call_procedure("my_table", "fast_forward", to_branch="audit_branch")
```

## Merge-on-Read (MoR) Support

*Note: Currently limited by underlying library support.*

IceFrame includes structure for MoR writers (`MoRWriter`) to handle Position Deletes and Equality Deletes as support becomes available.

## Garbage Collection

Parallelized cleanup operations for large tables.

```python
from iceframe.gc import GarbageCollector

gc = GarbageCollector(table)
gc.expire_snapshots(retain_last=5)
```
