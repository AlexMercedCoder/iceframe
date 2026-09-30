"""Regression tests for the post-0.13 release-hardening audit."""

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa
import pyarrow.orc as orc
import pytest

from iceframe.async_ops import AsyncIceFrame
from iceframe.branching import BranchManager
from iceframe.exceptions import ValidationError
from iceframe.incremental import IncrementalReader
from iceframe.ingest import read_orc
from iceframe.ingestion import DataIngestion
from iceframe.mcp_server import execute_query, read_documentation
from iceframe.query import QueryBuilder
from iceframe.rollback import RollbackManager
from iceframe.streaming import StreamingWriter


def test_mcp_documentation_rejects_path_traversal(tmp_path, monkeypatch):
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "safe.md").write_text("safe")
    (tmp_path / ".env").write_text("SECRET=must-not-leak")
    monkeypatch.setattr("iceframe.mcp_server._documentation_roots", lambda: [docs])

    assert read_documentation("safe.md") == "safe"
    with pytest.raises(ValidationError, match="inside the docs directory"):
        read_documentation("../.env")


def test_orc_reader_uses_real_pyarrow_api(tmp_path):
    path = tmp_path / "data.orc"
    orc.write_table(pa.table({"id": [1, 2]}), path)
    assert read_orc(str(path)).to_dicts() == [{"id": 1}, {"id": 2}]


def test_add_files_calls_current_pyiceberg_contract():
    table = MagicMock()
    DataIngestion(table).add_files(["file:///data/a.parquet"])
    table.add_files.assert_called_once_with(["file:///data/a.parquet"])


def test_rollback_timestamp_calls_current_pyiceberg_contract():
    table = MagicMock()
    manager = table.manage_snapshots.return_value
    manager.rollback_to_timestamp.return_value = manager
    RollbackManager(table).rollback_to_timestamp(1234)
    manager.rollback_to_timestamp.assert_called_once_with(1234)
    manager.commit.assert_called_once_with()


def test_list_branches_excludes_tags():
    from pyiceberg.table.refs import SnapshotRef, SnapshotRefType

    table = MagicMock()
    table.metadata.refs = {
        "main": SnapshotRef(snapshot_id=1, snapshot_ref_type=SnapshotRefType.BRANCH),
        "release": SnapshotRef(snapshot_id=1, snapshot_ref_type=SnapshotRefType.TAG),
    }
    assert BranchManager(table).list_branches() == ["main"]
    assert BranchManager(table).list_tags() == ["release"]


def test_streaming_auto_compaction_uses_public_facade():
    ice = MagicMock()
    writer = StreamingWriter(ice, "default.events", batch_size=1)
    writer.enable_auto_compaction(every_n_flushes=1)
    writer.write({"id": 1})
    ice.compact_data_files.assert_called_once_with("default.events")


def test_streaming_auto_compaction_surfaces_failure():
    ice = MagicMock()
    ice.compact_data_files.side_effect = RuntimeError("commit failed")
    writer = StreamingWriter(ice, "default.events", batch_size=1)
    writer.enable_auto_compaction(every_n_flushes=1)
    with pytest.raises(RuntimeError, match="Auto-compaction failed"):
        writer.write({"id": 1})


def test_scan_batches_honours_requested_batch_size(
    ice_frame, test_table_name, sample_schema, cleanup_table
):
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    data = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["a", "b", "c", "d", "e"],
            "age": pl.Series([1, 2, 3, 4, 5], dtype=pl.Int32),
            "created_at": [datetime.datetime.now()] * 5,
        }
    ).with_columns(pl.col("created_at").cast(pl.Datetime("us")))
    ice_frame.append_to_table(test_table_name, data)
    sizes = [batch.num_rows for batch in ice_frame.scan_batches(test_table_name, batch_size=2)]
    assert sizes == [2, 2, 1]


@pytest.mark.asyncio
async def test_async_context_closes_and_rejects_future_work(local_catalog_config):
    async with AsyncIceFrame(local_catalog_config, max_workers=1) as ice:
        assert ice.ice_frame is not None
    with pytest.raises(RuntimeError, match="closed"):
        await ice.read_table("default.anything")


def test_mcp_byte_limit_applies_to_complete_response(monkeypatch):
    import json

    frame = pl.DataFrame({"large_column_name": ["x" * 300 for _ in range(8)]})
    ice = MagicMock()
    ice.read_table.return_value = frame
    monkeypatch.setattr("iceframe.mcp_server.get_iceframe", lambda: ice)
    monkeypatch.setattr("iceframe.mcp_server.MAX_BYTES", 300)

    response = execute_query("default.events", limit=8)

    assert len(json.dumps(response, default=str).encode("utf-8")) <= 300
    assert response["truncated"] is True


def test_keyed_changes_distinguish_updates_from_inserts_and_deletes():
    before_scan = MagicMock()
    before_scan.to_arrow.return_value = pa.table({"id": [1, 2], "value": ["old", "gone"]})
    after_scan = MagicMock()
    after_scan.to_arrow.return_value = pa.table({"id": [1, 3], "value": ["new", "added"]})
    table = MagicMock()
    table.scan.side_effect = [before_scan, after_scan]

    changes = IncrementalReader(table).get_changes(1, 2, primary_keys=["id"])

    assert changes["added"].to_dicts() == [{"id": 3, "value": "added"}]
    assert changes["deleted"].to_dicts() == [{"id": 2, "value": "gone"}]
    assert changes["modified"].to_dicts() == [{"id": 1, "value": "new"}]


def test_query_explain_reports_pushdown_without_scanning():
    operations = MagicMock()
    table = MagicMock()
    table.schema.return_value.fields = [SimpleNamespace(name="id"), SimpleNamespace(name="value")]
    operations.get_table.return_value = table

    plan = QueryBuilder(operations, "default.events").select("id").limit(5).explain()

    assert plan["iceberg"]["selected_fields"] == ["id"]
    assert plan["iceberg"]["limit"] == 5
    table.scan.assert_not_called()


def test_create_view_sends_spec_compliant_view_version():
    """0.13 passed sql= to Catalog.create_view, which has no such parameter."""
    pytest.importorskip("pyiceberg.view.metadata", reason="view creation needs pyiceberg>=0.12")
    from iceframe.views import ViewManager

    catalog = MagicMock()
    ViewManager(catalog).create_view(
        "analytics.active_users",
        "SELECT id FROM analytics.users",
        schema={"id": pl.Int64},
        properties={"owner": "data"},
    )

    kwargs = catalog.create_view.call_args.kwargs
    assert kwargs["identifier"] == "analytics.active_users"
    assert kwargs["schema"] == pa.schema([("id", pa.int64())])
    assert kwargs["properties"] == {"owner": "data"}
    version = kwargs["view_version"]
    assert version.default_namespace == ("analytics",)
    representation = version.representations[0].root
    assert representation.sql == "SELECT id FROM analytics.users"
    assert representation.dialect == "spark"


def test_create_view_requires_schema():
    pytest.importorskip("pyiceberg.view.metadata", reason="view creation needs pyiceberg>=0.12")
    from iceframe.views import ViewManager

    catalog = MagicMock()
    with pytest.raises(ValidationError, match="schema"):
        ViewManager(catalog).create_view("default.v", "SELECT 1")
    catalog.create_view.assert_not_called()


def test_create_view_on_pyiceberg_without_view_models(monkeypatch):
    """PyIceberg 0.11 (still inside the supported range) has no view models."""
    import sys

    from iceframe.exceptions import UnsupportedOperationError
    from iceframe.views import ViewManager

    monkeypatch.setitem(sys.modules, "pyiceberg.view.metadata", None)
    with pytest.raises(UnsupportedOperationError, match="pyiceberg>=0.12"):
        ViewManager(MagicMock()).create_view("default.v", "SELECT 1", schema={"x": pl.Int64})
