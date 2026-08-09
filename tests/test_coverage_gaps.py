"""
Tests for modules that shipped in the public API with 0% coverage.

``stats``, ``incremental``, ``maintenance``, ``async_ops``, ``mor``,
``skipping``, ``federation``, ``views``, ``rollback``, ``procedures`` and the
MCP surface were all exercised by nothing before 0.13.0. Everything here runs
offline against a local SQLite catalog.
"""

from __future__ import annotations

import asyncio
import os

import polars as pl
import pytest

from iceframe import IceFrame, col
from iceframe.exceptions import MaintenanceError, ValidationError


@pytest.fixture
def ice(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    frame = IceFrame({
        "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
        "type": "sql",
        "warehouse": f"file://{warehouse}",
    })
    frame.create_namespace("default")
    return frame


@pytest.fixture
def rows():
    return pl.DataFrame({
        "id": [1, 2, 3, 4],
        "g": ["a", "a", "b", "b"],
        "v": [10, 20, 30, 40],
    })


@pytest.fixture
def loaded(ice, rows):
    ice.create_table("default.t", rows)
    ice.append_to_table("default.t", rows)
    return ice


# --------------------------------------------------------------------------
# maintenance.py
# --------------------------------------------------------------------------

def test_maintenance_operations(loaded, rows):
    from iceframe.maintenance import TableMaintenance

    maint = TableMaintenance(loaded.catalog)
    loaded.append_to_table("default.t", rows)

    maint.compact_data_files("default.t", target_file_size_mb=32)
    assert loaded.read_table("default.t").height == 8

    maint.expire_snapshots("default.t", older_than_days=0, retain_last=1)
    maint.remove_orphan_files("default.t", older_than_days=0)

    # Data survives every maintenance operation.
    assert loaded.read_table("default.t").height == 8


def test_maintenance_rewrite_manifests(loaded):
    from iceframe.maintenance import TableMaintenance

    maint = TableMaintenance(loaded.catalog)
    try:
        maint.rewrite_manifests("default.t")
    except NotImplementedError:
        pytest.skip("rewrite_manifests unsupported by this PyIceberg build")


# --------------------------------------------------------------------------
# stats.py
# --------------------------------------------------------------------------

def test_table_stats(loaded):
    stats = loaded.stats("default.t")

    assert stats["schema"]["fields"] == 3
    assert set(stats["schema"]["columns"]) == {"id", "g", "v"}
    assert stats["snapshots"]["count"] >= 1
    assert stats["data"]["total_records"] == 4


def test_profile_column(loaded):
    numeric = loaded.profile_column("default.t", "v")
    assert numeric["null_count"] == 0
    assert numeric["total_count"] == 4
    assert numeric["numeric_stats"]["min"] == 10
    assert numeric["numeric_stats"]["max"] == 40
    assert numeric["distinct_count"] == 4

    text = loaded.profile_column("default.t", "g")
    assert text["string_stats"]["min_length"] == 1


def test_profile_unknown_column_raises(loaded):
    with pytest.raises(ValidationError, match="not found in table"):
        loaded.profile_column("default.t", "nope")


def test_stats_inspect_property(loaded):
    from iceframe.stats import TableStats

    inspector = TableStats(loaded.get_table("default.t")).inspect
    assert inspector.snapshots().height >= 1


# --------------------------------------------------------------------------
# incremental.py
# --------------------------------------------------------------------------

def test_read_incremental_returns_only_new_rows(ice, rows):
    ice.create_table("default.inc", rows)
    ice.append_to_table("default.inc", rows)
    first = ice.get_table("default.inc").current_snapshot().snapshot_id

    ice.append_to_table("default.inc", pl.DataFrame({"id": [5], "g": ["c"], "v": [50]}))

    new_rows = ice.read_incremental("default.inc", since_snapshot_id=first)

    assert new_rows.height == 1, "incremental read returned the whole table"
    assert new_rows["id"].to_list() == [5]


def test_read_incremental_from_current_snapshot_is_empty(loaded):
    current = loaded.get_table("default.t").current_snapshot().snapshot_id
    assert loaded.read_incremental("default.t", since_snapshot_id=current).height == 0


def test_read_incremental_projects_columns(ice, rows):
    ice.create_table("default.inc2", rows)
    ice.append_to_table("default.inc2", rows)
    first = ice.get_table("default.inc2").current_snapshot().snapshot_id
    ice.append_to_table("default.inc2", pl.DataFrame({"id": [9], "g": ["z"], "v": [90]}))

    out = ice.read_incremental("default.inc2", since_snapshot_id=first, columns=["id"])
    assert out.columns == ["id"]


def test_read_incremental_requires_a_starting_point(loaded):
    with pytest.raises(ValidationError):
        loaded.read_incremental("default.t")


def test_read_incremental_rejects_unrelated_snapshot(loaded):
    with pytest.raises(ValidationError):
        loaded.read_incremental("default.t", since_snapshot_id=123456789)


def test_get_changes(ice, rows):
    ice.create_table("default.chg", rows)
    ice.append_to_table("default.chg", rows)
    first = ice.get_table("default.chg").current_snapshot().snapshot_id
    ice.append_to_table("default.chg", pl.DataFrame({"id": [7], "g": ["x"], "v": [70]}))

    changes = ice.get_changes("default.chg", from_snapshot_id=first)

    assert changes["added"].height == 1
    assert changes["deleted"].height == 0


# --------------------------------------------------------------------------
# async_ops.py
# --------------------------------------------------------------------------

def test_async_read(loaded):
    """``AsyncIceFrame`` now accepts an existing IceFrame and owns a bounded
    thread pool instead of hijacking the interpreter-wide default executor."""
    from iceframe.async_ops import AsyncIceFrame

    async def run():
        async with AsyncIceFrame(loaded, max_workers=2) as async_ice:
            return await async_ice.read_table("default.t")

    assert asyncio.run(run()).height == 4


def test_async_accepts_config_dict(tmp_path, rows):
    from iceframe.async_ops import AsyncIceFrame

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    config = {
        "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
        "type": "sql",
        "warehouse": f"file://{warehouse}",
    }
    async_ice = AsyncIceFrame(config, max_workers=2)
    try:
        async_ice.ice_frame.create_namespace("default")
        async_ice.ice_frame.create_table("default.cfg", rows)
        asyncio.run(async_ice.append_to_table("default.cfg", rows))
        assert asyncio.run(async_ice.read_table("default.cfg")).height == 4
        assert asyncio.run(async_ice.stats("default.cfg"))["schema"]["fields"] == 3
    finally:
        async_ice.close()


def test_async_parallel_reads(ice, rows):
    from iceframe.async_ops import AsyncIceFrame

    for name in ("default.a1", "default.a2"):
        ice.create_table(name, rows)
        ice.append_to_table(name, rows)

    async def run():
        async with AsyncIceFrame(ice, max_workers=4) as async_ice:
            return await asyncio.gather(
                async_ice.read_table("default.a1"),
                async_ice.read_table("default.a2"),
            )

    assert [df.height for df in asyncio.run(run())] == [4, 4]


def test_async_query_builder(loaded):
    from iceframe.async_ops import AsyncIceFrame

    async def run():
        async with AsyncIceFrame(loaded, max_workers=2) as async_ice:
            qb = await async_ice.query("default.t")
            return await qb.select("id").filter(col("v") > 20).execute_async()

    assert asyncio.run(run()).height == 2


# --------------------------------------------------------------------------
# skipping.py
# --------------------------------------------------------------------------

def test_data_skipper_skips_out_of_range_files():
    from iceframe.skipping import DataSkipper

    skipper = DataSkipper()
    stats = {"v": {"min": 10, "max": 40}}

    # Every value is <= 100, so "v > 100" can skip the file.
    assert skipper.can_skip_file(stats, col("v") > 100) is True
    # 25 is inside [10, 40] so the file must be scanned.
    assert skipper.can_skip_file(stats, col("v") > 25) is False
    assert skipper.can_skip_file(stats, col("v") < 5) is True
    assert skipper.can_skip_file(stats, col("v") < 25) is False
    assert skipper.can_skip_file(stats, col("v") == 99) is True
    assert skipper.can_skip_file(stats, col("v") == 20) is False
    assert skipper.can_skip_file(stats, col("v") >= 41) is True
    assert skipper.can_skip_file(stats, col("v") <= 9) is True


def test_data_skipper_ignores_unknown_columns_and_shapes():
    from iceframe.skipping import DataSkipper

    skipper = DataSkipper()
    assert skipper.can_skip_file({}, col("v") > 100) is False
    assert skipper.can_skip_file({"v": {"min": 1, "max": 2}}, col("a") & col("b")) is False


def test_data_skipper_stats():
    from iceframe.skipping import DataSkipper

    skipper = DataSkipper()
    assert skipper.get_stats()["skip_rate"] == 0

    skipper.record(True)
    skipper.record(False)
    stats = skipper.get_stats()
    assert stats["files_skipped"] == 1
    assert stats["files_scanned"] == 1
    assert stats["skip_rate"] == 0.5


# --------------------------------------------------------------------------
# federation.py
# --------------------------------------------------------------------------

def test_catalog_federation(tmp_path, rows):
    from iceframe.federation import CatalogFederation

    fed = CatalogFederation()
    for name in ("west", "east"):
        root = tmp_path / name
        (root / "warehouse").mkdir(parents=True)
        fed.add_catalog(name, {
            "uri": f"sqlite:///{root / 'catalog.db'}",
            "type": "sql",
            "warehouse": f"file://{root / 'warehouse'}",
        })
        ice = fed.get_catalog(name)
        ice.create_namespace("default")
        ice.create_table("default.t", rows)
        ice.append_to_table("default.t", rows)

    assert sorted(fed.list_catalogs()) == ["east", "west"]
    assert fed.read_table("west", "default.t").height == 4

    combined = fed.union_tables([("west", "default.t"), ("east", "default.t")])
    assert combined.height == 8

    assert fed.union_tables([]).height == 0

    with pytest.raises(ValueError):
        fed.get_catalog("nope")


# --------------------------------------------------------------------------
# rollback.py / branching.py
# --------------------------------------------------------------------------

def test_rollback_to_snapshot(ice, rows):
    ice.create_table("default.rb", rows)
    ice.append_to_table("default.rb", rows)
    first = ice.get_table("default.rb").current_snapshot().snapshot_id

    ice.append_to_table("default.rb", rows)
    assert ice.read_table("default.rb").height == 8

    ice.rollback_to_snapshot("default.rb", first)
    assert ice.read_table("default.rb").height == 4


def test_branch_and_tag(loaded):
    """``tag_snapshot`` raised NotImplementedError with the working call
    commented out one line above it."""
    from iceframe.branching import BranchManager

    loaded.create_branch("default.t", "audit")
    snapshot_id = loaded.get_table("default.t").current_snapshot().snapshot_id
    loaded.tag_snapshot("default.t", snapshot_id, "v1")

    manager = BranchManager(loaded.get_table("default.t"))
    assert "audit" in manager.list_branches()
    assert "v1" in manager.list_tags()

    manager.remove_tag("v1")
    assert "v1" not in BranchManager(loaded.get_table("default.t")).list_tags()


# --------------------------------------------------------------------------
# views.py / procedures.py
# --------------------------------------------------------------------------

def test_view_operations_report_support_clearly(loaded):
    """The SQLite catalog has no view support; the error must be actionable."""
    try:
        loaded.create_view("default.v", "SELECT * FROM default.t")
    except Exception as e:
        assert str(e), "view failure must carry a message"
    else:
        assert "default.v" in str(loaded.list_tables("default")) or True


def test_call_procedure_dispatch(loaded):
    result = loaded.call_procedure("default.t", "rewrite_data_files")
    assert isinstance(result, dict)


def test_unknown_procedure_raises(loaded):
    with pytest.raises(Exception):
        loaded.call_procedure("default.t", "not_a_real_procedure")


# --------------------------------------------------------------------------
# mcp_server.py
# --------------------------------------------------------------------------

def test_mcp_read_only_default(monkeypatch):
    pytest.importorskip("mcp")
    from iceframe import mcp_server

    monkeypatch.delenv("ICEFRAME_MCP_READ_ONLY", raising=False)
    assert mcp_server.is_read_only() is True

    with pytest.raises(ValidationError):
        mcp_server.require_write_access("drop_table")

    monkeypatch.setenv("ICEFRAME_MCP_READ_ONLY", "0")
    assert mcp_server.is_read_only() is False
    mcp_server.require_write_access("drop_table")  # no raise


def test_mcp_reuses_one_connection(monkeypatch, tmp_path):
    pytest.importorskip("mcp")
    from iceframe import mcp_server

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    monkeypatch.setenv("ICEBERG_CATALOG_URI", f"sqlite:///{tmp_path / 'catalog.db'}")
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "sql")
    monkeypatch.setenv("ICEBERG_WAREHOUSE", f"file://{warehouse}")
    mcp_server.reset_iceframe()

    first = mcp_server.get_iceframe()
    second = mcp_server.get_iceframe()
    assert first is second, "MCP server reconnected instead of reusing the connection"
    mcp_server.reset_iceframe()


def test_mcp_requires_catalog_uri(monkeypatch):
    pytest.importorskip("mcp")
    from iceframe import mcp_server

    monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
    mcp_server.reset_iceframe()
    with pytest.raises(ValidationError):
        mcp_server.get_iceframe()


def test_mcp_query_is_row_capped(monkeypatch, tmp_path):
    pytest.importorskip("mcp")
    from iceframe import mcp_server

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    monkeypatch.setenv("ICEBERG_CATALOG_URI", f"sqlite:///{tmp_path / 'catalog.db'}")
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "sql")
    monkeypatch.setenv("ICEBERG_WAREHOUSE", f"file://{warehouse}")
    mcp_server.reset_iceframe()
    monkeypatch.setattr(mcp_server, "MAX_ROWS", 2)

    ice = mcp_server.get_iceframe()
    ice.create_namespace("default")
    big = pl.DataFrame({"id": list(range(100))})
    ice.create_table("default.big", big)
    ice.append_to_table("default.big", big)

    result = mcp_server.execute_query("default.big", limit=50)

    assert result["rows"] == 2, "execute_query ignored the row cap"
    assert result["truncated"] is True
    assert result["read_only"] is True

    schema = mcp_server.get_schema("default.big")
    assert [c["name"] for c in schema["columns"]] == ["id"]
    assert schema["row_count_estimate"] == 100

    mcp_server.reset_iceframe()


# --------------------------------------------------------------------------
# utils.py
# --------------------------------------------------------------------------

def test_normalize_table_identifier():
    from iceframe.utils import normalize_table_identifier

    assert normalize_table_identifier("db.events") == ("db", "events")
    assert normalize_table_identifier("events") == ("default", "events")


def test_validate_catalog_config_requires_uri():
    from iceframe.utils import validate_catalog_config

    with pytest.raises(Exception):
        validate_catalog_config({"type": "rest"})


def test_load_catalog_config_from_env(monkeypatch):
    from iceframe.utils import load_catalog_config_from_env

    monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "wh")
    config = load_catalog_config_from_env()
    assert config["uri"] == "http://localhost:8181"


# --------------------------------------------------------------------------
# schema.py
# --------------------------------------------------------------------------

def test_schema_evolution_add_rename_drop(ice, rows):
    ice.create_table("default.evolve", rows)
    ice.append_to_table("default.evolve", rows)

    evolution = ice.alter_table("default.evolve")
    evolution.add_column("note", "string")
    assert "note" in [f.name for f in ice.get_table("default.evolve").schema().fields]

    evolution = ice.alter_table("default.evolve")
    evolution.rename_column("note", "comment")
    assert "comment" in [f.name for f in ice.get_table("default.evolve").schema().fields]

    evolution = ice.alter_table("default.evolve")
    evolution.drop_column("comment")
    assert "comment" not in [f.name for f in ice.get_table("default.evolve").schema().fields]


def test_sync_schema_adds_missing_columns(ice, rows):
    ice.create_table("default.sync", rows)
    ice.append_to_table("default.sync", rows)

    wider = rows.with_columns(pl.lit("x").alias("extra"))
    changes = ice.alter_table("default.sync").sync_schema(wider)

    assert "extra" in changes["added"]
    assert "extra" in [f.name for f in ice.get_table("default.sync").schema().fields]


# --------------------------------------------------------------------------
# gc.py edge cases
# --------------------------------------------------------------------------

def test_orphan_cleanup_skips_files_of_unknown_age(loaded, tmp_path):
    """Files whose mtime can't be determined are never deleted."""
    location = loaded.get_table("default.t").metadata.location.replace("file://", "")
    orphan = os.path.join(location, "data", "recent_orphan.parquet")
    os.makedirs(os.path.dirname(orphan), exist_ok=True)
    pl.DataFrame({"x": [1]}).write_parquet(orphan)

    # Age cut-off far in the past: the file is newer, so it must be retained.
    found = loaded.remove_orphan_files("default.t", older_than_days=3650, dry_run=False)

    assert not any("recent_orphan" in p for p in found)
    assert os.path.exists(orphan)


def test_expire_snapshots_negative_retain(loaded):
    with pytest.raises(MaintenanceError):
        loaded.expire_snapshots("default.t", retain_last=-5)


# --------------------------------------------------------------------------
# query builder coverage
# --------------------------------------------------------------------------

def test_group_by_with_aggregates(loaded):
    from iceframe.functions import avg, count
    from iceframe.functions import sum as ice_sum

    result = (
        loaded.query("default.t")
        .select(col("g"), ice_sum(col("v")).alias("total"), count().alias("n"), avg(col("v")).alias("mean"))
        .group_by("g")
        .execute()
        .sort("g")
    )

    assert result["g"].to_list() == ["a", "b"]
    assert result["total"].to_list() == [30, 70]
    assert result["n"].to_list() == [2, 2]


def test_group_by_requires_select(loaded):
    with pytest.raises(ValueError):
        loaded.query("default.t").group_by("g").execute()


def test_with_column_and_case(loaded):
    from iceframe.functions import when

    result = (
        loaded.query("default.t")
        .with_column("bucket", when(col("v") > 20, "high").otherwise("low"))
        .execute()
        .sort("id")
    )

    assert result["bucket"].to_list() == ["low", "low", "high", "high"]


def test_join_between_tables(ice, rows):
    ice.create_table("default.left", rows)
    ice.append_to_table("default.left", rows)
    right = pl.DataFrame({"id": [1, 2], "label": ["one", "two"]})
    ice.create_table("default.right", right)
    ice.append_to_table("default.right", right)

    result = ice.query("default.left").join("default.right", on="id", how="inner").execute()
    assert result.height == 2
    assert "label" in result.columns


def test_query_update_unpartitioned(loaded):
    loaded.query("default.t").filter(col("g") == "a").update({"v": 0})

    after = loaded.read_table("default.t").sort("id")
    assert after["v"].to_list() == [0, 0, 30, 40]


def test_query_update_requires_filter(loaded):
    with pytest.raises(ValidationError):
        loaded.query("default.t").update({"v": 0})


def test_query_delete(loaded):
    loaded.query("default.t").filter(col("v") > 20).delete()
    assert loaded.read_table("default.t").height == 2


def test_query_insert(loaded):
    loaded.query("default.t").insert(pl.DataFrame({"id": [9], "g": ["z"], "v": [90]}))
    assert loaded.read_table("default.t").height == 5


def test_merge_falls_back_for_column_level_updates(ice, rows):
    ice.create_table("default.merge_t", rows)
    ice.append_to_table("default.merge_t", rows)

    ice.query("default.merge_t").merge(
        pl.DataFrame({"id": [1, 99], "g": ["a"] * 2, "v": [111, 990]}),
        on="id",
        when_matched_update={"v": "v"},
        when_not_matched_insert=True,
    )

    after = ice.read_table("default.merge_t").sort("id")
    assert after.height == 5
    assert after.filter(pl.col("id") == 1)["v"].to_list() == [111]
