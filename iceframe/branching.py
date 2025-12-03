"""
Branching and tagging support for IceFrame.
"""

from typing import Optional, List, Dict, Any
from pyiceberg.table import Table

class BranchManager:
    """
    Manages branches and tags for Iceberg tables.
    
    Note: Branching is an Iceberg v2 feature. Not all catalogs support it yet.
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def create_branch(self, branch_name: str, snapshot_id: Optional[int] = None) -> None:
        """
        Create a new branch.
        
        Args:
            branch_name: Name of the branch
            snapshot_id: Snapshot ID to branch from (defaults to current)
        """
        try:
            if snapshot_id is None:
                current = self.table.current_snapshot()
                snapshot_id = current.snapshot_id if current else None
                
            if snapshot_id is None:
                raise ValueError("No snapshot available to create branch from")
                
            # Try PyIceberg 0.6.0+ API
            if hasattr(self.table, "manage_snapshots"):
                # Signature is (snapshot_id, branch_name)
                self.table.manage_snapshots().create_branch(snapshot_id, branch_name).commit()
            else:
                raise NotImplementedError("Branch creation requires PyIceberg 0.6.0+")
                
        except AttributeError:
            raise NotImplementedError("Branching not supported by this PyIceberg version or catalog")
            
    def tag_snapshot(self, snapshot_id: int, tag_name: str) -> None:
        """
        Tag a specific snapshot.
        
        Args:
            snapshot_id: Snapshot ID to tag
            tag_name: Name for the tag
        """
        try:
            # PyIceberg API for tags
            # self.table.manage_snapshots().create_tag(tag_name, snapshot_id).commit()
            raise NotImplementedError("Snapshot tagging requires PyIceberg 0.6.0+ with catalog support")
        except AttributeError:
            raise NotImplementedError("Tagging not supported by this PyIceberg version or catalog")
            
    def list_branches(self) -> List[str]:
        """
        List all branches.
        
        Returns:
            List of branch names
        """
        try:
            # PyIceberg stores refs in table metadata
            if hasattr(self.table.metadata, "refs"):
                return list(self.table.metadata.refs.keys())
            return ["main"]
        except AttributeError:
            return ["main"]

    def fast_forward(self, branch: str, to_branch: str) -> None:
        """
        Fast-forward a branch to another branch (e.g. main -> audit_branch).
        
        Args:
            branch: Branch to update (e.g. 'main')
            to_branch: Branch to fast-forward to
        """
        try:
            if hasattr(self.table, "manage_snapshots"):
                # Get snapshot ID of target branch
                refs = self.table.metadata.refs
                if to_branch not in refs:
                    raise ValueError(f"Branch '{to_branch}' not found")
                
                target_snapshot_id = refs[to_branch].snapshot_id
                
                # Update reference
                ms = self.table.manage_snapshots()
                if hasattr(ms, "replace_branch"):
                    ms.replace_branch(branch, target_snapshot_id).commit()
                else:
                    # Native implementation for older PyIceberg versions
                    # We manually construct the update and commit via transaction
                    try:
                        from pyiceberg.table.update.snapshot import SetSnapshotRefUpdate
                        
                        # Create transaction
                        txn = self.table.transaction()
                        
                        # Create update
                        update = SetSnapshotRefUpdate(
                            snapshot_id=target_snapshot_id,
                            ref_name=branch,
                            type="branch"
                        )
                        
                        # Inject update (hack for older versions)
                        if isinstance(txn._updates, tuple):
                            txn._updates = txn._updates + (update,)
                        else:
                            txn._updates.append(update)
                            
                        # Commit
                        txn.commit_transaction()
                        
                    except ImportError:
                        raise NotImplementedError("Fast-forward requires PyIceberg 0.6.0+ or SetSnapshotRefUpdate")
            else:
                raise NotImplementedError("Fast-forward requires PyIceberg 0.6.0+")
        except AttributeError:
            raise NotImplementedError("Branching not supported by this PyIceberg version")
