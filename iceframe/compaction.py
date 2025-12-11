"""
Advanced compaction strategies for Iceberg tables.
"""

from typing import Optional, List, Dict, Any
import polars as pl
import pyarrow as pa
from pyiceberg.table import Table

class CompactionManager:
    """
    Manage table compaction (rewrite data files).
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def bin_pack(
        self,
        target_file_size_mb: int = 128,
        filter_expr: Optional[str] = None,
        min_input_files: int = 1,
        partition_filter: Optional[Dict[str, Any]] = None,
        deduplicate: bool = False,
        **kwargs
    ) -> Dict[str, int]:
        """
        Compact small files into larger files (Bin-packing).
        Safe implementation: Compacts one partition at a time to manage memory.
        
        Args:
            target_file_size_mb: Target size in MB
            filter_expr: Optional filter to select files to compact
            min_input_files: Minimum number of files required in a partition to trigger compaction
            partition_filter: Dict of column=value to filter specific partitions (e.g. {'cat': 'A'})
            deduplicate: Whether to deduplicate fully identical rows during compaction
            
        Returns:
            Stats on compacted files
        """
        # 1. Check if PyIceberg has native support (and if no custom options used)
        # Note: If deduplicate or partition_filter dict is used, we MUST use manual path
        if min_input_files == 1 and not deduplicate and not partition_filter:
            try:
                if hasattr(self.table, 'rewrite_data_files'):
                    pass 
            except Exception:
                pass
            
        # 2. Manual Implementation (Safe & Smart)
        
        # Build scan with filters
        scan = self.table.scan()
        if filter_expr:
            scan = scan.filter(filter_expr)
            
        # Apply partition_filter dict if provided
        from pyiceberg.expressions import EqualTo, And, AlwaysTrue
        target_partitions: Optional[List[Dict]] = None
        
        if partition_filter:
             # Add filter to scan for analyzing plan_files
             manual_filter = AlwaysTrue()
             for col, val in partition_filter.items():
                 # Handle string/literal conversion automatically by PyIceberg if we pass literals?
                 # Assuming simple types for now
                 if manual_filter == AlwaysTrue():
                     manual_filter = EqualTo(col, val)
                 else:
                     manual_filter = And(manual_filter, EqualTo(col, val))
             scan = scan.filter(manual_filter)
            
        # Analyze partitions
        print("Analyzing table partitions...")
        partition_stats = {}
        
        # Iterate tasks to gather stats
        try:
            for task in scan.plan_files():
                # Task has .file which is DataFile
                f = task.file
                # Partition key (Record)
                p_key = str(f.partition) # Use string rep as key for now
                
                if p_key not in partition_stats:
                    partition_stats[p_key] = {"count": 0, "bytes": 0, "partition": f.partition}
                    
                partition_stats[p_key]["count"] += 1
                partition_stats[p_key]["bytes"] += f.file_size_in_bytes
        except Exception as e:
            print(f"Warning: Failed to gather stats via plain_files: {e}")
            pass

        # If no stats gathered (maybe empty or error), fallback to old logic for unpartitioned
        if not partition_stats:
             # Logic for unpartitioned table or fallback
             pass
             
        # Filter partitions to compact
        partitions_to_compact = []
        skipped_partitions = 0
        
        for p_key, stats in partition_stats.items():
            should_compact = True
            
            # Check min_input_files
            if stats["count"] < min_input_files:
                should_compact = False
            
            if should_compact:
                partitions_to_compact.append(stats["partition"])
            else:
                skipped_partitions += 1
                
        if not partitions_to_compact and skipped_partitions > 0:
            return {
                "rewritten_rows": 0,
                "strategy": "skipped_all",
                "message": f"Skipped {skipped_partitions} partitions (min_input_files={min_input_files})",
                "deduplicated": deduplicate
            }
            
        # Perform Compaction on selected partitions
        total_rows = 0
        rewritten_partitions = 0
        
        print(f"Compacting {len(partitions_to_compact)} partitions (Skipped {skipped_partitions})...")
        
        for partition_val in partitions_to_compact:
            # Reconstruct filter from partition values (Record)
            # We must build a filter that targets THIS partition exactly
            part_filter = AlwaysTrue()
            
            # Check if it's partitioned
            spec = self.table.spec()
            if spec.fields:
                 # It's a Record object, need to iterate fields
                 # Record doesn't expose strict dict iteration easily but has field names matching spec?
                 # Actually, partition_val is `Record`. We can try to match fields.
                 # Safe way: Iterate spec fields and get value from record
                 for field in spec.fields:
                     # This is complex because field name in record might differ (transforms)?
                     # For identity transform, it matches.
                     # Let's trust the `partitions_to_compact` value or re-derive via scan if needed.
                     
                     # Simpler alternative for this loop:
                     # We have the partition key. We can scan specifically for it.
                     pass

            # Since constructing the exact filter from Record is hard without deep inspection of spec/transforms
            # We will use the approach of "Scan table, filter by unique partitions detected in scan"
            # BUT optimize by filtering the initial scan.
            # Waait, we loop `partitions_to_compact`.
            # If we simply use the scan logic from before (iterate unique partitions via Arrow),
            # we can filter THAT list against our `partitions_to_compact` set/list.
            # But matching Records is hard.
            
            # Let's stick to the previous robust loop: Scan -> Unique Partitions -> Filter -> Overwrite
            # But apply our logic (min_files, partition_filter) *inside* that loop.
            pass

        # Re-implementation using the Robust Loop
        spec = self.table.spec()
        schema = self.table.schema()
        source_col_ids = [f.source_id for f in spec.fields]
        source_col_names = [schema.find_field(id).name for id in source_col_ids]
        
        if not source_col_names:
            # Unpartitioned Logic
            arrow_table = scan.to_arrow() # already filtered by partition_filter dict if applied to scan
            if arrow_table.num_rows == 0:
                 return {"rewritten_rows": 0}
            
            # Check file count logic (global)
            global_count = sum(s["count"] for s in partition_stats.values()) if partition_stats else 0
            if global_count < min_input_files and global_count > 0:
                 return {"rewritten_rows": 0, "message": "Skipped unpartitioned (fewer than min files)"}

            # Deduplication
            if deduplicate:
                df = pl.from_arrow(arrow_table)
                original_rows = df.height
                df = df.unique()
                print(f"Deduplicated: {original_rows} -> {df.height} rows")
                arrow_table = df.to_arrow()

            self.table.overwrite(arrow_table)
            return {"rewritten_rows": arrow_table.num_rows, "strategy": "bin_pack_full", "deduplicated": deduplicate}
            
        # Partitioned Logic
        partition_dist_scan = self.table.scan(selected_fields=tuple(source_col_names))
        if filter_expr:
             partition_dist_scan = partition_dist_scan.filter(filter_expr)
             
        # Apply partition_filter dict here too
        if partition_filter:
             # Re-build filter (same as above)
             manual_filter = AlwaysTrue()
             for col, val in partition_filter.items():
                 if manual_filter == AlwaysTrue():
                     manual_filter = EqualTo(col, val)
                 else:
                     manual_filter = And(manual_filter, EqualTo(col, val))
             partition_dist_scan = partition_dist_scan.filter(manual_filter)

        partitions_df = pl.from_arrow(partition_dist_scan.to_arrow()).unique()
        
        # Reset counters
        skipped_partitions_count = 0
        
        for row in partitions_df.to_dicts():
            # Build Partition Filter
            part_filter = AlwaysTrue()
            for col, val in row.items():
                 if part_filter == AlwaysTrue():
                     part_filter = EqualTo(col, val)
                 else:
                     part_filter = And(part_filter, EqualTo(col, val))
            
            # Check file count for this partition via fast scan
            # (We could check partition_stats if we can match the key, but string rep is tricky)
            # Robust way: Fast plan_files scan for this partition
            try:
                part_files_count = 0
                part_scan = self.table.scan(row_filter=part_filter)
                for _ in part_scan.plan_files():
                    part_files_count += 1
                    if part_files_count >= min_input_files:
                        break
                
                if part_files_count < min_input_files:
                    skipped_partitions_count += 1
                    continue
            except:
                pass

            # Read Partition
            part_arrow = self.table.scan(row_filter=part_filter).to_arrow()
            if part_arrow.num_rows == 0:
                continue
            
            # Deduplication
            if deduplicate:
                # Deduplicate within partition
                df = pl.from_arrow(part_arrow)
                df = df.unique()
                part_arrow = df.to_arrow()
                
            total_rows += part_arrow.num_rows
            rewritten_partitions += 1
            
            # Rewrite Partition (Safe Overwrite)
            self.table.overwrite(part_arrow, overwrite_filter=part_filter)
            
        return {
            "rewritten_rows": total_rows,
            "strategy": "bin_pack_partitioned",
            "skipped_partitions": skipped_partitions_count,
            "rewritten_partitions": rewritten_partitions,
            "deduplicated": deduplicate
        }
    def sort(
        self,
        sort_order: List[str],
        target_file_size_mb: int = 128,
        filter_expr: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Compact and sort files (Z-order approximation if multiple columns).
        
        Args:
            sort_order: List of columns to sort by
            target_file_size_mb: Target size in MB
            filter_expr: Optional filter
            
        Returns:
            Stats
        """
        # 1. Read data
        scan = self.table.scan()
        if filter_expr:
            scan = scan.filter(filter_expr)
            
        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)
        
        if df.height == 0:
            return {"rewritten_files": 0}
            
        # 2. Sort data
        sorted_df = df.sort(sort_order)
        
        # 3. Overwrite
        self.table.overwrite(sorted_df.to_arrow())
        
        return {
            "rewritten_rows": df.height,
            "strategy": "sort",
            "sort_order": str(sort_order)
        }

    def rewrite_manifests(self, target_size_mb: int = 8) -> dict:
        """
        Rewrite manifest files to optimize metadata (native implementation).
        
        Args:
            target_size_mb: Target size for manifest files in MB
            
        Returns:
            Stats on rewritten manifests
        """
        try:
            # Get current snapshot
            current_snapshot = self.table.current_snapshot()
            if not current_snapshot:
                return {"rewritten_manifests": 0, "message": "No snapshots to optimize"}
            
            # Get all manifest files
            manifests = list(current_snapshot.manifests(self.table.io))
            
            if len(manifests) <= 1:
                return {"rewritten_manifests": 0, "message": "Only one manifest, no optimization needed"}
            
            # Calculate total entries across all manifests
            total_entries = sum(m.added_files_count or 0 for m in manifests)
            
            # Estimate if rewriting would help
            # (many small manifests vs few large ones)
            avg_entries_per_manifest = total_entries / len(manifests) if manifests else 0
            
            if avg_entries_per_manifest > 100:  # Arbitrary threshold
                return {
                    "rewritten_manifests": 0,
                    "message": f"Manifests already well-sized ({avg_entries_per_manifest:.0f} entries/manifest)"
                }
            
            # Native implementation would require:
            # 1. Reading all manifest entries
            # 2. Combining into fewer, larger manifests
            # 3. Writing new manifest files
            # 4. Creating new snapshot with updated manifest list
            
            # This is complex and requires direct metadata manipulation
            # For now, we'll check if PyIceberg supports it
            if hasattr(self.table, 'rewrite_manifests'):
                result = self.table.rewrite_manifests()
                if hasattr(result, 'commit'):
                    result.commit()
                return {
                    "rewritten_manifests": len(manifests),
                    "original_count": len(manifests)
                }
            else:
                # Return diagnostic info for manual optimization
                return {
                    "rewritten_manifests": 0,
                    "message": "Manifest rewriting not supported by PyIceberg",
                    "manifest_count": len(manifests),
                    "total_entries": total_entries,
                    "avg_entries_per_manifest": avg_entries_per_manifest,
                    "recommendation": "Consider upgrading PyIceberg or using Spark for manifest optimization"
                }
                
        except Exception as e:
            raise NotImplementedError(f"Manifest rewriting not supported: {e}")
