"""
Garbage collection and cleanup.
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Optional, Set

from pyiceberg.table import Table

from iceframe.exceptions import MaintenanceError

logger = logging.getLogger(__name__)

#: Metadata file suffixes that are never data files and must never be
#: classified as orphans just because no manifest references them. Puffin
#: statistics and partition-stats files live alongside the manifests and were
#: previously deleted by ``remove_orphan_files`` on local filesystems.
_STATISTICS_SUFFIXES = (".puffin", ".stats", ".parquet.stats")


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
        max_workers: int = 4,
    ) -> List[int]:
        """
        Expire old snapshots using PyIceberg's native maintenance API.

        The previous implementation called ``Table.expire_snapshots(...)``,
        which does not exist on ``pyiceberg.table.Table`` — so the ``hasattr``
        guard always failed and the method raised ``NotImplementedError``
        whenever it actually had work to do. The real API is
        ``Table.maintenance.expire_snapshots()`` returning a builder with
        ``.by_id()`` / ``.older_than()`` / ``.commit()``.

        Snapshots are sorted by ``timestamp_ms`` before ``retain_last`` is
        applied — ``Table.snapshots()`` is not guaranteed to be chronological.

        Branch and tag heads are protected by PyIceberg itself and are never
        expired, so the current snapshot always survives.

        Args:
            older_than_ms: Only expire snapshots strictly older than this
                millisecond epoch timestamp. ``None`` means no age limit.
            retain_last: Always keep at least this many of the newest
                snapshots. ``0`` means "no retention floor" (protected refs
                still survive).
            max_workers: Accepted for API compatibility; snapshot expiry is a
                single metadata commit, so this is unused.

        Returns:
            The list of snapshot ids that were requested for expiry.
        """
        if retain_last < 0:
            raise MaintenanceError("retain_last must be >= 0")

        snapshots = sorted(self.table.snapshots(), key=lambda s: s.timestamp_ms)

        if retain_last:
            if len(snapshots) <= retain_last:
                return []
            candidates = snapshots[:-retain_last]
        else:
            candidates = list(snapshots)

        to_expire = [
            s.snapshot_id
            for s in candidates
            if older_than_ms is None or s.timestamp_ms < older_than_ms
        ]

        if not to_expire:
            return []

        try:
            expire = self.table.maintenance.expire_snapshots()
        except AttributeError as e:  # pragma: no cover - very old PyIceberg
            raise MaintenanceError(
                "Snapshot expiration requires PyIceberg >= 0.10 "
                "(Table.maintenance.expire_snapshots). "
                f"Installed version does not provide it: {e}"
            ) from e

        expired: List[int] = []
        for snapshot_id in to_expire:
            try:
                expire = expire.by_id(snapshot_id)
                expired.append(snapshot_id)
            except ValueError as e:
                # Protected (branch/tag head) or already gone. Skip it rather
                # than aborting the whole run.
                logger.debug("Skipping snapshot %s: %s", snapshot_id, e)

        if not expired:
            return []

        try:
            expire.commit()
        except Exception as e:
            raise MaintenanceError(f"Failed to expire snapshots: {e}") from e

        logger.info("Expired %d snapshot(s) from %s", len(expired), self.table.name())
        return expired

    def _valid_metadata_files(self) -> Set[str]:
        """Every metadata/statistics file the table still needs."""
        valid: Set[str] = set()

        if self.table.metadata_location:
            valid.add(self.table.metadata_location)

        for log_entry in self.table.metadata.metadata_log:
            valid.add(log_entry.metadata_file)

        for snapshot in self.table.snapshots():
            if snapshot.manifest_list:
                valid.add(snapshot.manifest_list)
            for manifest in snapshot.manifests(self.table.io):
                valid.add(manifest.manifest_path)

        # Puffin / statistics files are referenced from table metadata, not
        # from manifests. Missing them here meant orphan cleanup happily
        # deleted a table's statistics.
        for attr in ("statistics", "partition_statistics"):
            for entry in getattr(self.table.metadata, attr, None) or []:
                path = getattr(entry, "statistics_path", None) or getattr(
                    entry, "statistics_file_path", None
                )
                if path:
                    valid.add(path)

        return valid

    def _file_mtime_ms(self, file_path: str) -> Optional[float]:
        """
        Best-effort modification time in epoch milliseconds.

        Local paths use ``os.stat``. Object stores go through the FileIO's
        underlying fsspec/PyArrow filesystem, which does expose mtime — the old
        code only handled ``file://`` and therefore skipped *every* candidate on
        S3/GCS/ADLS, making the whole operation a no-op there.
        """
        local_path = file_path[7:] if file_path.startswith("file://") else file_path
        if os.path.isabs(local_path) and os.path.exists(local_path):
            try:
                return os.stat(local_path).st_mtime * 1000
            except OSError as e:
                logger.debug("os.stat failed for %s: %s", file_path, e)

        io = self.table.io

        # fsspec-backed FileIO (s3fs / gcsfs / adlfs)
        fs = getattr(io, "fs", None) or getattr(io, "get_fs", None)
        try:
            if callable(fs):
                scheme = file_path.split("://", 1)[0] if "://" in file_path else "file"
                fs = fs(scheme)
            if fs is not None and hasattr(fs, "info"):
                info = fs.info(file_path)
                for key in ("mtime", "LastModified", "last_modified", "modification_time"):
                    value = info.get(key) if isinstance(info, dict) else None
                    if value is None:
                        continue
                    if isinstance(value, datetime):
                        return value.timestamp() * 1000
                    if isinstance(value, (int, float)):
                        return float(value) * 1000
        except Exception as e:
            logger.debug("fsspec stat failed for %s: %s", file_path, e)

        # PyArrow FileSystem-backed FileIO
        try:
            from pyarrow.fs import FileSystem

            filesystem, path = FileSystem.from_uri(file_path)
            info = filesystem.get_file_info(path)
            if info.mtime is not None:
                return info.mtime.timestamp() * 1000
        except Exception as e:
            logger.debug("pyarrow stat failed for %s: %s", file_path, e)

        return None

    def remove_orphan_files(
        self,
        older_than_ms: Optional[int] = None,
        max_workers: int = 4,
        dry_run: bool = True,
    ) -> List[str]:
        """
        Find (and optionally delete) files under the table location that no
        live snapshot or metadata entry references.

        ``dry_run`` defaults to ``True``: this operation deletes files
        permanently, so callers must opt in to the destructive behaviour.

        Args:
            older_than_ms: Only consider files last modified before this
                millisecond epoch timestamp. Files whose age can't be
                determined are always skipped.
            max_workers: Parallelism for the delete phase.
            dry_run: When ``True`` (default) nothing is deleted; the candidate
                list is returned for inspection.

        Returns:
            The list of orphan file paths (deleted, unless ``dry_run``).
        """
        try:
            # 1. Every data file referenced by ANY live snapshot. Older
            #    snapshots remain valid for time travel until expired.
            referenced_files: Set[str] = set()
            seen_manifests: Set[str] = set()
            for snapshot in self.table.snapshots():
                try:
                    for manifest in snapshot.manifests(self.table.io):
                        if manifest.manifest_path in seen_manifests:
                            continue
                        seen_manifests.add(manifest.manifest_path)
                        for entry in manifest.fetch_manifest_entry(self.table.io):
                            referenced_files.add(entry.data_file.file_path)
                except Exception as e:
                    # We cannot safely classify orphans without knowing what
                    # this snapshot references — abort rather than guess.
                    logger.error(
                        "Could not read manifests for snapshot %s: %s. "
                        "Aborting orphan cleanup to avoid deleting live data.",
                        snapshot.snapshot_id,
                        e,
                    )
                    return []

            valid_metadata_files = self._valid_metadata_files()

            io = self.table.io
            table_location = self.table.metadata.location
            all_files: Set[str] = set()
            all_files.update(self._list_files(f"{table_location}/data"))
            all_files.update(self._list_files(f"{table_location}/metadata"))

            # 3. Classify
            orphans: List[str] = []
            for file_path in sorted(all_files):
                if file_path in referenced_files or file_path in valid_metadata_files:
                    continue
                if file_path.endswith(_STATISTICS_SUFFIXES):
                    # Statistics sidecars: never delete on suffix alone.
                    logger.debug("Preserving statistics-like file %s", file_path)
                    continue

                if older_than_ms:
                    mtime = self._file_mtime_ms(file_path)
                    if mtime is None:
                        logger.warning(
                            "Could not determine age of %s; skipping for safety", file_path
                        )
                        continue
                    if mtime >= older_than_ms:
                        continue

                orphans.append(file_path)

            if dry_run or not orphans:
                if orphans:
                    logger.info("Dry run: %d orphan file(s) identified", len(orphans))
                return orphans

            # 4. Delete, honouring max_workers (previously accepted and ignored).
            def _delete(path: str) -> None:
                try:
                    io.delete(path)
                except Exception as e:
                    if path.startswith("file://"):
                        try:
                            os.remove(path[7:])
                            return
                        except OSError as os_err:
                            logger.error("Failed to delete %s: %s", path, os_err)
                            return
                    logger.error("Failed to delete %s: %s", path, e)

            if max_workers and max_workers > 1:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(_delete, p) for p in orphans]
                    for future in as_completed(futures):
                        future.result()
            else:
                for path in orphans:
                    _delete(path)

            logger.info("Removed %d orphan file(s)", len(orphans))
            return orphans

        except MaintenanceError:
            raise
        except Exception as e:
            raise MaintenanceError(f"Orphan file removal failed: {e}") from e

    def _list_files(self, location: str) -> Set[str]:
        """List every non-directory file under ``location``."""
        io = self.table.io
        results: Set[str] = set()

        path_to_list = location[7:] if location.startswith("file://") else location

        for candidate in (path_to_list, location):
            try:
                for file_info in io.list_prefix(candidate):
                    if not file_info.is_directory:
                        results.add(file_info.path)
                if results:
                    return results
            except Exception as e:
                logger.debug("list_prefix(%s) failed: %s", candidate, e)

        # Fallback for local filesystems where FileIO doesn't implement listing.
        if not results and location.startswith("file://"):
            local_path = location[7:]
            if os.path.exists(local_path):
                for root, _dirs, files in os.walk(local_path):
                    for name in files:
                        results.add(f"file://{os.path.join(root, name)}")

        return results


def utc_ms(dt: datetime) -> int:
    """Convert a datetime to a millisecond epoch timestamp (UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


__all__ = ["GarbageCollector", "utc_ms"]
