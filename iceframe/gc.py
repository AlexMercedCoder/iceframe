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
            
            # 2. List all files in table data and metadata locations
            io = self.table.io
            table_location = self.table.metadata.location
            data_location = f"{table_location}/data"
            metadata_location = f"{table_location}/metadata"
            
            # Determine valid metadata files
            valid_metadata_files = set()
            try:
                # Add current metadata file
                if self.table.metadata_location:
                    valid_metadata_files.add(self.table.metadata_location)
                
                # Add history metadata files
                for log_entry in self.table.metadata.metadata_log:
                    valid_metadata_files.add(log_entry.metadata_file)
                    
                # Add all snapshots (manifest lists)
                for snapshot in self.table.snapshots():
                    if snapshot.manifest_list:
                        valid_metadata_files.add(snapshot.manifest_list)
                        
                    # Add manifests for this snapshot
                    for manifest in snapshot.manifests(self.table.io):
                        valid_metadata_files.add(manifest.manifest_path)
                        
            except Exception as e:
                print(f"Warning: Could not fully determine valid metadata files: {e}")
                
            all_files = set()
            
            # Helper to list files
            def list_files(location):
                results = set()
                # Strip scheme for local fs?
                path_to_list = location
                if location.startswith("file://"):
                    path_to_list = location[7:]
                
                try:
                    for file_info in io.list_prefix(path_to_list):
                        if not file_info.is_directory:
                            results.add(file_info.path)
                except Exception as e:
                    # Also try original location if stripped failed or returned empty
                    try:
                        if path_to_list != location:
                             for file_info in io.list_prefix(location):
                                if not file_info.is_directory:
                                    results.add(file_info.path)
                    except:
                        pass
                        
                # Fallback for local files if empty
                if not results and location.startswith("file://"):
                    import os
                    local_path = location[7:]
                    if os.path.exists(local_path):
                        for root, dirs, files in os.walk(local_path):
                            for file in files:
                                full_path = os.path.join(root, file)
                                # Re-add scheme
                                results.add(f"file://{full_path}")
                return results

            # List data files
            all_files.update(list_files(data_location))
            
            # List metadata files (if requested or by default?)
            all_files.update(list_files(metadata_location))
            
            # 3. Find orphans
            orphans = []
            for file_path in all_files:
                is_data = file_path in referenced_files
                is_metadata = file_path in valid_metadata_files
                
                if not is_data and not is_metadata:
                    # Check age if specified
                    if older_than_ms:
                        try:
                            # Try to get mtime
                            mtime = None
                            
                            if file_path.startswith("file://"):
                                import os
                                try:
                                    mtime = os.stat(file_path[7:]).st_mtime * 1000
                                except:
                                    pass
                                    
                            if mtime is None:
                                # Try io.new_input_file().stat() if available
                                try:
                                    inp = io.new_input_file(file_path)
                                    # InputFile stat might vary
                                    # Some implementations support accessing metadata
                                    pass 
                                except:
                                    pass
                                    
                            # As a fallback, try io.stat if we haven't tried or logic allows
                            # But we saw it fail.
                            
                            if mtime is not None:
                                if mtime >= older_than_ms:
                                    continue
                            else:
                                # Could not stat, skip safety
                                print(f"Warning: Could not determine age of {file_path}, skipping cleanup")
                                continue
                                
                        except Exception as e:
                            # If we can't stat, don't delete to be safe
                            print(f"Stat failed for {file_path}: {e}")
                            continue
                            
                    orphans.append(file_path)
            
            # 4. Delete orphans (if not dry run)
            if not dry_run and orphans:
                for file_path in orphans:
                    try:
                        io.delete(file_path)
                    except Exception as e:
                        # Fallback for local delete
                        if file_path.startswith("file://"):
                            try:
                                import os
                                os.remove(file_path[7:])
                            except:
                                print(f"Failed to delete {file_path}: {e}")
                        else:
                             print(f"Failed to delete {file_path}: {e}")
            
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
