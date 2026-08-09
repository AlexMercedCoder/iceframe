"""
Branching and tagging support for IceFrame.
"""

from typing import List, Optional

from pyiceberg.table import Table

from iceframe.exceptions import UnsupportedOperationError


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
            raise NotImplementedError("Branching not supported by this PyIceberg version or catalog") from None

    def tag_snapshot(
        self,
        snapshot_id: int,
        tag_name: str,
        max_ref_age_ms: Optional[int] = None,
    ) -> None:
        """
        Tag a specific snapshot.

        This used to unconditionally raise ``NotImplementedError`` with the
        working call commented out one line above it. PyIceberg's
        ``ManageSnapshots.create_tag(snapshot_id, tag_name)`` does the job.

        Args:
            snapshot_id: Snapshot ID to tag
            tag_name: Name for the tag
            max_ref_age_ms: Optional maximum age for the tag reference
        """
        try:
            manager = self.table.manage_snapshots()
            if max_ref_age_ms is not None:
                manager = manager.create_tag(snapshot_id, tag_name, max_ref_age_ms)
            else:
                manager = manager.create_tag(snapshot_id, tag_name)
            manager.commit()
        except AttributeError as e:
            raise UnsupportedOperationError(
                f"Tagging is not supported by this PyIceberg version or catalog: {e}"
            ) from e

    def remove_tag(self, tag_name: str) -> None:
        """Remove a tag."""
        try:
            self.table.manage_snapshots().remove_tag(tag_name).commit()
        except AttributeError as e:
            raise UnsupportedOperationError(
                f"Tag removal is not supported by this PyIceberg version or catalog: {e}"
            ) from e

    def list_tags(self) -> List[str]:
        """List all tag names."""
        from pyiceberg.table.refs import SnapshotRefType

        refs = getattr(self.table.metadata, "refs", {}) or {}
        return [
            name
            for name, ref in refs.items()
            if ref.snapshot_ref_type == SnapshotRefType.TAG
        ]

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
                        raise NotImplementedError("Fast-forward requires PyIceberg 0.6.0+ or SetSnapshotRefUpdate") from None
            else:
                raise NotImplementedError("Fast-forward requires PyIceberg 0.6.0+")
        except AttributeError:
            raise NotImplementedError("Branching not supported by this PyIceberg version") from None
