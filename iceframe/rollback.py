"""
Rollback and snapshot management.
"""

from typing import Optional
from pyiceberg.table import Table

class RollbackManager:
    """
    Manage table rollbacks and snapshot settings.
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def rollback_to_snapshot(self, snapshot_id: int) -> None:
        """
        Rollback table state to a specific snapshot.
        
        Args:
            snapshot_id: Snapshot ID to rollback to
        """
        try:
            if hasattr(self.table, "manage_snapshots"):
                self.table.manage_snapshots().rollback_to_snapshot(snapshot_id).commit()
            else:
                raise NotImplementedError("Rollback requires PyIceberg 0.6.0+")
        except AttributeError:
            raise NotImplementedError("Rollback not supported by this PyIceberg version")
            
    def rollback_to_timestamp(self, timestamp_ms: int) -> None:
        """
        Rollback table state to a specific timestamp.
        
        Args:
            timestamp_ms: Timestamp in milliseconds
        """
        try:
            if hasattr(self.table, "manage_snapshots"):
                self.table.manage_snapshots().rollback_to_time(timestamp_ms).commit()
            else:
                raise NotImplementedError("Rollback requires PyIceberg 0.6.0+")
        except AttributeError:
            raise NotImplementedError("Rollback not supported by this PyIceberg version")
            
    def set_current_snapshot(self, snapshot_id: int) -> None:
        """
        Explicitly set the current snapshot (cherry-pick/branch manipulation).
        
        Args:
            snapshot_id: Snapshot ID to set as current
        """
        try:
            if hasattr(self.table, "manage_snapshots"):
                self.table.manage_snapshots().set_current_snapshot(snapshot_id).commit()
            else:
                raise NotImplementedError("Setting current snapshot requires PyIceberg 0.6.0+")
        except AttributeError:
            raise NotImplementedError("Operation not supported by this PyIceberg version")
