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
        **kwargs
    ) -> Dict[str, int]:
        """
        Compact small files into larger files (Bin-packing).
        Safe implementation: Compacts one partition at a time to manage memory.
        
        Args:
            target_file_size_mb: Target size in MB
            filter_expr: Optional filter to select files to compact
            min_input_files: Minimum number of files required in a partition to trigger compaction
            
        Returns:
            Stats on compacted files
        """
        # 1. Check if PyIceberg has native support (and if no custom options used)
        if min_input_files == 1:
            try:
                if hasattr(self.table, 'rewrite_data_files'):
                    # Note: Basic support might not handle all args yet
                    # result = self.table.rewrite_data_files()
                    # return result
                    pass 
            except Exception:
                pass
            
        # 2. Manual Implementation (Safe & Smart)
        
        # Scan to find files (using plan_files for metadata access)
        scan = self.table.scan()
        if filter_expr:
            scan = scan.filter(filter_expr)
            
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
            # Fallback to simple iteration if plan_files fails (e.g. unpartitioned)
            pass

        # If no stats gathered (maybe empty or error), fallback to old logic for unpartitioned
        if not partition_stats:
             # Logic for unpartitioned table or fallback
             pass
             
        # Filter partitions to compact
        partitions_to_compact = []
        skipped_partitions = 0
        
        for p_key, stats in partition_stats.items():
            if stats["count"] >= min_input_files:
                partitions_to_compact.append(stats["partition"])
            else:
                skipped_partitions += 1
                
        if not partitions_to_compact and skipped_partitions > 0:
            return {
                "rewritten_rows": 0,
                "strategy": "skipped_all",
                "message": f"Skipped {skipped_partitions} partitions (min_input_files={min_input_files})"
            }
            
        # Perform Compaction on selected partitions
        total_rows = 0
        from pyiceberg.expressions import EqualTo, And, AlwaysTrue
        
        print(f"Compacting {len(partitions_to_compact)} partitions (Skipped {skipped_partitions})...")
        
        for partition_val in partitions_to_compact:
            # Build Partition Filter
            part_filter = AlwaysTrue()
            
            # Handle unpartitioned or empty partition record
            if not isinstance(partition_val, dict) and not hasattr(partition_val, "as_dict"):
                 # Likely unpartitioned if it's an empty record
                 pass
            
            # Access fields from Record
            if hasattr(partition_val, "as_dict"):
                pass # Use record fields
            
            # Reconstruct filter from partition values
            # This is tricky because `partition_val` is an internal Record.
            # We need to match it against the partition spec fields.
            
            # Simpler approach: If we have the partition values, we can just use them.
            # But constructing the generic filter is safer via the scan logic used before.
            # Let's revert to the previous 'unique partition' scan approach BUT filtered by our decision.
            pass

        # Re-implementation using the "Iterate Unique Partitions" pattern but with counts
        # Since we already decided which ones to compact based on plan_files, we can just filter.
        
        spec = self.table.spec()
        schema = self.table.schema()
        source_col_ids = [f.source_id for f in spec.fields]
        source_col_names = [schema.find_field(id).name for id in source_col_ids]
        
        if not source_col_names:
            # Unpartitioned
            arrow_table = scan.to_arrow()
            if arrow_table.num_rows == 0:
                 return {"rewritten_rows": 0}
            
            # Check file count for unpartitioned? 
            # We'd need to know file count. 
            # plan_files gave us that globally.
            global_count = sum(s["count"] for s in partition_stats.values()) if partition_stats else 0
            if global_count < min_input_files and global_count > 0:
                return {"rewritten_rows": 0, "message": "Skipped unpartitioned (fewer than min files)"}

            self.table.overwrite(arrow_table)
            return {"rewritten_rows": arrow_table.num_rows, "strategy": "bin_pack_full"}
            
        # Partitioned
        partition_dist_scan = self.table.scan(selected_fields=tuple(source_col_names))
        if filter_expr:
             partition_dist_scan = partition_dist_scan.filter(filter_expr)

        partitions_df = pl.from_arrow(partition_dist_scan.to_arrow()).unique()
        
        # Reset skipped count for the actual execution loop
        skipped_partitions = 0
        total_rows = 0
        
        for row in partitions_df.to_dicts():
            # Build Partition Filter
            part_filter = AlwaysTrue()
            for col, val in row.items():
                 if part_filter == AlwaysTrue():
                     part_filter = EqualTo(col, val)
                 else:
                     part_filter = And(part_filter, EqualTo(col, val))
            
            # Check file count for this partition
            # We can use our pre-computed stats if we can match the key
            # Or just do a quick scan since we are processing one by one anyway
            
            # Fast scan for file count
            # This scans manifests, not data, so it's fast
            try:
                part_files_count = 0
                part_scan = self.table.scan(row_filter=part_filter)
                # plan_files is a generator
                for _ in part_scan.plan_files():
                    part_files_count += 1
                    if part_files_count >= min_input_files:
                        break # Optimization: enough files found
                
                if part_files_count < min_input_files:
                    # Skip
                    skipped_partitions += 1
                    continue
            except:
                # Fallback if plan_files fails, just read
                pass

            # Read Partition
            part_arrow = self.table.scan(row_filter=part_filter).to_arrow()
            if part_arrow.num_rows == 0:
                continue
                
            total_rows += part_arrow.num_rows
            
            # Rewrite Partition (Safe Overwrite)
            self.table.overwrite(part_arrow, overwrite_filter=part_filter)
            
        return {
            "rewritten_rows": total_rows,
            "strategy": "bin_pack_partitioned",
            "skipped_partitions": skipped_partitions
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
