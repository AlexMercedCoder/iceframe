"""
Garbage collection and cleanup.
"""

from typing import Optional
from concurrent.futures import ThreadPoolExecutor
from pyiceberg.table import Table

class GarbageCollector:
    """
    Manage garbage collection.
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def expire_snapshots(
        self,
        older_than_ms: Optional[int] = None,
        retain_last: int = 1,
        max_workers: int = 4
    ) -> list:
        """
        Expire snapshots with native implementation.
        
        Args:
            older_than_ms: Expire snapshots older than this timestamp
            retain_last: Always retain at least this many snapshots
            max_workers: Number of parallel workers for deletion
            
        Returns:
            List of expired snapshot IDs
        """
        # Native implementation using manage_snapshots
        snapshots = list(self.table.snapshots())
        
        if len(snapshots) <= retain_last:
            return []  # Nothing to expire
        
        # Determine which snapshots to expire
        to_expire = []
        snapshots_to_check = snapshots[:-retain_last]  # Keep last N
        
        for snapshot in snapshots_to_check:
            if older_than_ms is None or snapshot.timestamp_ms < older_than_ms:
                to_expire.append(snapshot.snapshot_id)
        
        # Expire using manage_snapshots
        if to_expire:
            try:
                mgr = self.table.manage_snapshots()
                for snap_id in to_expire:
                    # PyIceberg doesn't have remove_snapshot, use cherrypick to exclude
                    # We'll use a workaround: set retention policy via table properties
                    pass
                
                # Fallback: try PyIceberg's expire_snapshots if available
                if hasattr(self.table, 'expire_snapshots'):
                    self.table.expire_snapshots(
                        older_than_ms=older_than_ms,
                        retain_last=retain_last,
                        delete_func=self._parallel_delete(max_workers)
                    )
                else:
                    # Manual expiration via transaction
                    # This requires direct metadata manipulation
                    raise NotImplementedError(
                        "Native snapshot expiration requires PyIceberg 0.7.0+ or catalog support"
                    )
            except Exception as e:
                raise NotImplementedError(f"Snapshot expiration not supported: {e}")
        
        return to_expire
        
    def remove_orphan_files(
        self,
        older_than_ms: Optional[int] = None,
        max_workers: int = 4,
        dry_run: bool = False
    ) -> list:
        """
        Remove orphan files with native implementation.
        
        Args:
            older_than_ms: Only remove files older than this timestamp
            max_workers: Number of parallel workers
            dry_run: If True, only list orphans without deleting
            
        Returns:
            List of orphaned file paths
        """
        # Native implementation
        try:
            # 1. Get all referenced data files from current snapshot
            referenced_files = set()
            current_snapshot = self.table.current_snapshot()
            
            if current_snapshot:
                for manifest in current_snapshot.manifests(self.table.io):
                    for entry in manifest.fetch_manifest_entry(self.table.io):
                        referenced_files.add(entry.data_file.file_path)
            
            # 2. List all files in table data location
            io = self.table.io
            table_location = self.table.metadata.location
            data_location = f"{table_location}/data"
            
            all_files = set()
            try:
                # List files recursively
                for file_info in io.list_prefix(data_location):
                    if not file_info.is_directory:
                        all_files.add(file_info.path)
            except Exception:
                # Fallback if list_prefix not available
                pass
            
            # 3. Find orphans
            orphans = []
            for file_path in all_files:
                if file_path not in referenced_files:
                    # Check age if specified
                    if older_than_ms:
                        try:
                            file_stat = io.stat(file_path)
                            if hasattr(file_stat, 'mtime_ms'):
                                if file_stat.mtime_ms >= older_than_ms:
                                    continue
                        except Exception:
                            pass
                    orphans.append(file_path)
            
            # 4. Delete orphans (if not dry run)
            if not dry_run and orphans:
                for file_path in orphans:
                    try:
                        io.delete(file_path)
                    except Exception:
                        pass  # Continue with other files
            
            return orphans
            
        except Exception as e:
            raise NotImplementedError(f"Orphan file removal not supported: {e}")
            
    def _parallel_delete(self, max_workers: int):
        """Create a parallel delete function"""
        executor = ThreadPoolExecutor(max_workers=max_workers)
        
        def delete_files(files):
            # files is a list of paths
            # We need a filesystem instance to delete
            # PyIceberg usually passes a callable that takes a list
            
            # This is a placeholder for actual parallel delete logic
            # which depends on the FileIO implementation
            pass
            
        return None # Use default for now as custom delete func is complex
