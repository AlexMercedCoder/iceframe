"""
Merge-on-Read (MoR) write support.

**Status: copy-on-write only.** PyIceberg 0.11 does not expose a public API for
writing position or equality delete files, so the two delete-file writers below
raise :class:`~iceframe.exceptions.UnsupportedOperationError`. :meth:`MoRWriter.delete_where`
is a thin, honest wrapper over Iceberg's copy-on-write delete — it does not
silently swallow failures the way it used to.
"""

import logging
from typing import Any, Dict, List, Union

import pyarrow as pa
from pyiceberg.table import Table

from iceframe.cache import invalidate_query_cache
from iceframe.exceptions import UnsupportedOperationError

logger = logging.getLogger(__name__)


class MoRWriter:
    """
    Writer for Merge-on-Read (delete files).

    Merge-on-read delete *writes* are not implemented — see the module
    docstring. Reading tables that already contain delete files is handled by
    PyIceberg transparently and needs nothing from this class.
    """

    def __init__(self, table: Table):
        self.table = table

    def write_position_deletes(self, data_file_path: str, positions: List[int]) -> None:
        """
        Write a position delete file.

        Raises:
            UnsupportedOperationError: always — PyIceberg does not expose the
                delete-file writer publicly.
        """
        raise UnsupportedOperationError(
            "Writing position deletes requires low-level PyIceberg IO access that is "
            "not publicly exposed in PyIceberg 0.11. Use delete_where() for a "
            "copy-on-write delete, or Spark/Flink for merge-on-read deletes."
        )

    def write_equality_deletes(
        self,
        equality_ids: List[int],
        rows: Union[pa.Table, List[Dict[str, Any]]],
    ) -> None:
        """
        Write an equality delete file.

        Raises:
            UnsupportedOperationError: always — see
                :meth:`write_position_deletes`.
        """
        raise UnsupportedOperationError(
            "Writing equality deletes requires low-level PyIceberg IO access that is "
            "not publicly exposed in PyIceberg 0.11. Use delete_where() for a "
            "copy-on-write delete, or Spark/Flink for merge-on-read deletes."
        )

    def delete_where(self, filter_expr: str) -> None:
        """
        Delete rows matching ``filter_expr`` using Iceberg's copy-on-write
        delete.

        Args:
            filter_expr: An Iceberg predicate (string or ``BooleanExpression``).

        Raises:
            Whatever PyIceberg raises. This used to catch every exception and
            ``pass``, so a delete that failed reported success.
        """
        logger.debug("Copy-on-write delete on %s: %s", self.table.name(), filter_expr)
        self.table.delete(filter_expr)
        invalidate_query_cache(self.table.name())
