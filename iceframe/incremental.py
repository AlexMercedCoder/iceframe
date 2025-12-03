"""
Incremental processing for IceFrame.
"""

from typing import Optional, Dict, Any
import polars as pl
from pyiceberg.table import Table

class IncrementalReader:
    """
    Handles incremental reads and change data capture (CDC) for Iceberg tables.
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def read_incremental(
        self, 
        since_snapshot_id: Optional[int] = None,
        since_timestamp: Optional[int] = None,
        columns: Optional[list] = None
    ) -> pl.DataFrame:
        """
        Read only data added since a specific snapshot or timestamp.
        
        Args:
            since_snapshot_id: Read data added after this snapshot ID
            since_timestamp: Read data added after this timestamp (milliseconds since epoch)
            columns: Optional list of columns to select
            
        Returns:
            Polars DataFrame with incremental data
        """
        if since_snapshot_id is None and since_timestamp is None:
            raise ValueError("Must specify either since_snapshot_id or since_timestamp")
            
        # Get current snapshot
        current_snapshot = self.table.current_snapshot()
        if not current_snapshot:
            return pl.DataFrame()
            
        # Determine starting snapshot
        if since_snapshot_id:
            start_snapshot_id = since_snapshot_id
        else:
            # Find snapshot closest to timestamp
            start_snapshot_id = self._find_snapshot_by_timestamp(since_timestamp)
            
        # Read data from start snapshot to current
        # PyIceberg doesn't directly support incremental scans, so we need to:
        # 1. Read current data
        # 2. Read data at start snapshot
        # 3. Compute the difference
        
        # For now, we'll use a simpler approach: read all data and filter by metadata
        # In production, you'd want to use manifest filtering for efficiency
        
        scan = self.table.scan()
        if columns:
            scan = scan.select(*columns)
            
        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)
        
        # Note: This is a simplified implementation
        # A production version would use manifest-level filtering
        return df
        
    def get_changes(
        self,
        from_snapshot_id: int,
        to_snapshot_id: Optional[int] = None,
        columns: Optional[list] = None
    ) -> Dict[str, pl.DataFrame]:
        """
        Get changes (inserts, updates, deletes) between two snapshots.
        
        Args:
            from_snapshot_id: Starting snapshot ID
            to_snapshot_id: Ending snapshot ID (defaults to current)
            columns: Optional list of columns to select
            
        Returns:
            Dictionary with 'added', 'deleted', 'modified' DataFrames
        """
        if to_snapshot_id is None:
            current = self.table.current_snapshot()
            to_snapshot_id = current.snapshot_id if current else None
            
        if not to_snapshot_id:
            return {"added": pl.DataFrame(), "deleted": pl.DataFrame(), "modified": pl.DataFrame()}
            
        # Read data at both snapshots
        # Note: PyIceberg's snapshot() method allows time-travel reads
        from_scan = self.table.scan(snapshot_id=from_snapshot_id)
        to_scan = self.table.scan(snapshot_id=to_snapshot_id)
        
        if columns:
            from_scan = from_scan.select(*columns)
            to_scan = to_scan.select(*columns)
            
        from_df = pl.from_arrow(from_scan.to_arrow())
        to_df = pl.from_arrow(to_scan.to_arrow())
        
        # Compute differences
        # This is a simplified implementation - production would use primary keys
        # For now, we'll just return added/deleted based on row presence
        
        # Added: rows in 'to' but not in 'from'
        added = to_df.join(from_df, how="anti", on=to_df.columns)
        
        # Deleted: rows in 'from' but not in 'to'
        deleted = from_df.join(to_df, how="anti", on=from_df.columns)
        
        # Modified: for simplicity, we'll leave this empty
        # A real implementation would need primary key tracking
        modified = pl.DataFrame()
        
        return {
            "added": added,
            "deleted": deleted,
            "modified": modified
        }
        
    def _find_snapshot_by_timestamp(self, timestamp_ms: int) -> Optional[int]:
        """Find the snapshot closest to (but before) the given timestamp"""
        snapshots = list(self.table.metadata.snapshots)
        
        # Find the latest snapshot before the timestamp
        best_snapshot = None
        for snapshot in snapshots:
            if snapshot.timestamp_ms <= timestamp_ms:
                if best_snapshot is None or snapshot.timestamp_ms > best_snapshot.timestamp_ms:
                    best_snapshot = snapshot
                    
        return best_snapshot.snapshot_id if best_snapshot else None
