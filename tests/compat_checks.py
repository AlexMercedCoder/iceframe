"""
Feature checks run against every catalog in the compatibility matrix.

Each check takes an IceFrame, a scratch namespace and a temp directory, and
raises on failure. ``tests/test_catalog_compat.py`` runs them under pytest and
``scripts/compat_matrix.py`` turns the same results into
``docs/compatibility.md``, so the published matrix is whatever the tests
actually observed.
"""

import os
import uuid
from collections.abc import Callable

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from iceframe import IceFrame, UnsupportedOperationError, col

ROWS = pl.DataFrame({"id": [1, 2, 3, 4], "cat": ["a", "a", "b", "b"], "v": [1.0, 2.0, 3.0, 4.0]})

Check = Callable[[IceFrame, str, str], None]


class CatalogLacksFeature(Exception):
    """The catalog itself does not offer the feature; IceFrame is not at fault."""


def _table(ice: IceFrame, ns: str, rows: pl.DataFrame = ROWS) -> str:
    name = f"{ns}.t_{uuid.uuid4().hex[:8]}"
    ice.create_table(name, rows)
    ice.append_to_table(name, rows)
    return name


def _ids(ice: IceFrame, name: str, **kwargs) -> list[int]:
    return sorted(ice.read_table(name, **kwargs)["id"].to_list())


def check_namespaces(ice, ns, tmp):
    child = f"{ns}_x{uuid.uuid4().hex[:6]}"
    ice.create_namespace(child)
    assert any(child in ".".join(n) for n in ice.list_namespaces())
    ice.drop_namespace(child)


def check_create_append_read(ice, ns, tmp):
    name = _table(ice, ns)
    assert _ids(ice, name) == [1, 2, 3, 4]
    assert name.split(".")[-1] in " ".join(ice.list_tables(ns))


def check_pushdown_read(ice, ns, tmp):
    name = _table(ice, ns)
    out = ice.query(name).filter(col("id") > 2).select("id").execute()
    assert sorted(out["id"].to_list()) == [3, 4]


def check_overwrite(ice, ns, tmp):
    name = _table(ice, ns)
    ice.overwrite_table(name, ROWS.filter(pl.col("id") == 1))
    assert _ids(ice, name) == [1]


def check_filtered_overwrite(ice, ns, tmp):
    name = _table(ice, ns)
    ice.overwrite_table(
        name,
        pl.DataFrame({"id": [9], "cat": ["a"], "v": [9.0]}),
        overwrite_filter="cat = 'a'",
    )
    assert _ids(ice, name) == [3, 4, 9]


def check_delete(ice, ns, tmp):
    name = _table(ice, ns)
    ice.delete_from_table(name, "id = 2")
    assert _ids(ice, name) == [1, 3, 4]


def check_upsert(ice, ns, tmp):
    name = _table(ice, ns)
    result = ice.upsert(
        name, pl.DataFrame({"id": [1, 5], "cat": ["z", "c"], "v": [10.0, 5.0]}), join_cols=["id"]
    )
    assert result == {"rows_updated": 1, "rows_inserted": 1}
    assert _ids(ice, name) == [1, 2, 3, 4, 5]


def check_transaction(ice, ns, tmp):
    name = _table(ice, ns)
    with ice.transaction(name) as txn:
        txn.append(ROWS.to_arrow())
        txn.append(ROWS.to_arrow())
    assert len(_ids(ice, name)) == 12

    # A failure inside the block commits nothing.
    try:
        with ice.transaction(name) as txn:
            txn.append(ROWS.to_arrow())
            raise RuntimeError("abort")
    except RuntimeError:
        pass
    assert len(_ids(ice, name)) == 12


def check_schema_evolution(ice, ns, tmp):
    name = _table(ice, ns)
    ice.alter_table(name).add_column("extra", "string")
    ice.alter_table(name).rename_column("v", "value")
    columns = ice.read_table(name).columns
    assert "extra" in columns and "value" in columns and "v" not in columns


def check_partition_evolution(ice, ns, tmp):
    name = _table(ice, ns)
    ice.evolve_partition(name).add_identity_partition("cat")
    ice.append_to_table(name, ROWS)
    assert len(ice.get_table(name).spec().fields) == 1
    assert len(_ids(ice, name)) == 8


def check_time_travel(ice, ns, tmp):
    name = _table(ice, ns)
    first = ice.get_table(name).current_snapshot().snapshot_id
    ice.append_to_table(name, ROWS)
    assert len(_ids(ice, name, snapshot_id=first)) == 4
    assert len(_ids(ice, name)) == 8


def check_rollback(ice, ns, tmp):
    name = _table(ice, ns)
    first = ice.get_table(name).current_snapshot().snapshot_id
    ice.append_to_table(name, ROWS)
    ice.rollback_to_snapshot(name, first)
    assert len(_ids(ice, name)) == 4


def check_branch_and_tag(ice, ns, tmp):
    name = _table(ice, ns)
    snap = ice.get_table(name).current_snapshot().snapshot_id
    ice.create_branch(name, "audit")
    ice.tag_snapshot(name, snap, "v1")
    ice.append_to_table(name, ROWS, branch="audit")
    refs = ice.get_table(name).refs()
    assert {"main", "audit", "v1"} <= set(refs)
    assert len(_ids(ice, name)) == 4


def check_metadata_tables(ice, ns, tmp):
    name = _table(ice, ns)
    inspector = ice.inspect(name)
    assert inspector.snapshots().height >= 1
    assert inspector.files().height >= 1
    assert inspector.history().height >= 1


def check_incremental_read(ice, ns, tmp):
    name = _table(ice, ns)
    since = ice.get_table(name).current_snapshot().snapshot_id
    ice.append_to_table(name, ROWS.with_columns(pl.col("id") + 10))
    assert sorted(ice.read_incremental(name, since_snapshot_id=since)["id"].to_list()) == [
        11,
        12,
        13,
        14,
    ]


def check_compaction(ice, ns, tmp):
    name = _table(ice, ns)
    for _ in range(3):
        ice.append_to_table(name, ROWS)
    ice.compact_data_files(name)
    assert len(_ids(ice, name)) == 16


def check_expire_snapshots(ice, ns, tmp):
    name = _table(ice, ns)
    ice.append_to_table(name, ROWS)
    ice.expire_snapshots(name, older_than_days=0, retain_last=1)
    assert len(ice.get_table(name).snapshots()) == 1
    assert len(_ids(ice, name)) == 8


def check_orphan_file_scan(ice, ns, tmp):
    name = _table(ice, ns)
    candidates = ice.remove_orphan_files(name, older_than_days=0)
    assert isinstance(candidates, list)


def check_add_files(ice, ns, tmp):
    name = f"{ns}.t_{uuid.uuid4().hex[:8]}"
    ice.create_table(name, ROWS)
    location = ice.get_table(name).location()
    path = f"{location}/data/added-{uuid.uuid4().hex[:6]}.parquet"
    local = path.removeprefix("file://")
    os.makedirs(os.path.dirname(local), exist_ok=True)
    pq.write_table(ROWS.to_arrow().cast(ice.get_table(name).schema().as_arrow()), local)
    ice.add_files(name, [path])
    assert _ids(ice, name) == [1, 2, 3, 4]


def check_views(ice, ns, tmp):
    view = f"{ns}.v_{uuid.uuid4().hex[:8]}"
    try:
        ice.create_view(view, "SELECT 1 AS one", schema=pa.schema([("one", pa.int32())]))
    except UnsupportedOperationError as e:
        raise CatalogLacksFeature(str(e)) from e
    assert ice.catalog.view_exists(view)
    ice.drop_view(view)
    assert not ice.catalog.view_exists(view)


def check_drop_table(ice, ns, tmp):
    name = _table(ice, ns)
    ice.drop_table(name)
    assert not ice.table_exists(name)


CHECKS: dict[str, Check] = {
    "Namespaces": check_namespaces,
    "Create, append, read": check_create_append_read,
    "Query builder pushdown": check_pushdown_read,
    "Overwrite": check_overwrite,
    "Filtered overwrite": check_filtered_overwrite,
    "Delete": check_delete,
    "Upsert": check_upsert,
    "Transactions": check_transaction,
    "Schema evolution": check_schema_evolution,
    "Partition evolution": check_partition_evolution,
    "Time travel": check_time_travel,
    "Rollback": check_rollback,
    "Branches and tags": check_branch_and_tag,
    "Metadata tables": check_metadata_tables,
    "Incremental read": check_incremental_read,
    "Compaction": check_compaction,
    "Expire snapshots": check_expire_snapshots,
    "Orphan file scan": check_orphan_file_scan,
    "Add existing files": check_add_files,
    "Views": check_views,
    "Drop table": check_drop_table,
}


def catalogs_from_env(tmp_root: str) -> dict[str, dict]:
    """
    The catalogs to check. The local SQLite catalog is always included; REST
    catalogs are added when their environment variables are set.
    """
    warehouse = os.path.join(tmp_root, "sqlite-warehouse")
    os.makedirs(warehouse, exist_ok=True)
    catalogs = {
        "PyIceberg SQL (SQLite)": {
            "type": "sql",
            "uri": f"sqlite:///{os.path.join(tmp_root, 'compat.db')}",
            "warehouse": f"file://{warehouse}",
        }
    }
    if os.environ.get("ICEFRAME_COMPAT_REST_URI"):
        catalogs["Iceberg REST reference"] = {
            "type": "rest",
            "uri": os.environ["ICEFRAME_COMPAT_REST_URI"],
        }
    if os.environ.get("ICEFRAME_COMPAT_POLARIS_URI"):
        catalogs["Apache Polaris"] = {
            "type": "rest",
            "uri": os.environ["ICEFRAME_COMPAT_POLARIS_URI"],
            "credential": os.environ.get("ICEFRAME_COMPAT_POLARIS_CREDENTIAL", "root:s3cr3t"),
            "scope": "PRINCIPAL_ROLE:ALL",
            "warehouse": os.environ.get("ICEFRAME_COMPAT_POLARIS_WAREHOUSE", "iceframe"),
        }
    return catalogs
