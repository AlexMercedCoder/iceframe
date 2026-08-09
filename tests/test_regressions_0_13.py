"""
Regression tests for issues fixed in IceFrame 0.13.0.

Every test here pins a specific bug from ``roadmap.md`` and runs offline
against a local SQLite catalog. If one of these fails, a shipped correctness
fix has regressed.
"""

from __future__ import annotations

import polars as pl
import pyarrow as pa
import pytest

from iceframe import IceFrame, col
from iceframe.exceptions import (
    CompactionError,
    IceFrameError,
    MaintenanceError,
    SchemaError,
    ValidationError,
)


@pytest.fixture
def ice(tmp_path):
    """A fresh IceFrame backed by a tmp_path SQLite catalog."""
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
def six_rows():
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "g": ["a", "a", "b", "b", "c", "c"],
        "v": [10, 20, 30, 40, 50, 60],
    })


# ---------------------------------------------------------------------------
# 0.1 — filtered compaction destroyed every non-matching row
# ---------------------------------------------------------------------------

def test_filtered_compaction_preserves_non_matching_rows(ice, six_rows):
    """
    Roadmap finding 1. ``compact_data_files(filter_expr="v > 30")`` called
    ``table.overwrite(arrow)`` with no ``overwrite_filter``; overwrite defaults
    to AlwaysTrue, so the whole table was replaced by the 3 matching rows.
    Reproduced as 6 rows -> 3 rows, reported as success.
    """
    table = "default.compact_filtered"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)
    assert ice.read_table(table).height == 6

    stats = ice.compact_data_files(table, filter_expr="v > 30")

    after = ice.read_table(table).sort("id")
    assert after.height == 6, (
        f"filtered compaction deleted non-matching rows: {after.height} of 6 left"
    )
    assert after["id"].to_list() == [1, 2, 3, 4, 5, 6]
    assert after["v"].to_list() == [10, 20, 30, 40, 50, 60]
    assert stats["scoped"] is True
    assert stats["rewritten_rows"] == 3


def test_unfiltered_compaction_still_rewrites_everything(ice, six_rows):
    """The whole-table path must remain a full rewrite."""
    table = "default.compact_full"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    stats = ice.compact_data_files(table)

    assert ice.read_table(table).height == 6
    assert stats["rewritten_rows"] == 6
    assert stats["scoped"] is False
    assert stats["strategy"] == "bin_pack_full"


def test_partition_filter_compaction_preserves_other_partitions(ice, six_rows):
    """``partition_filter`` must scope the overwrite the same way."""
    table = "default.compact_partfilter"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    ice.compact_data_files(table, partition_filter={"g": "a"})

    after = ice.read_table(table).sort("id")
    assert after.height == 6, "partition-scoped compaction dropped other partitions"


def test_z_order_optimize_with_filter_preserves_rows(ice, six_rows):
    """
    Roadmap finding 1 (second site). ``z_order_optimize`` had the identical
    unscoped-overwrite shape at compaction.py:404.
    """
    table = "default.zorder_filtered"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    result = ice.z_order_optimize(table, ["v"], filter_expr="v > 30")

    after = ice.read_table(table).sort("id")
    assert after.height == 6, (
        f"filtered z-order deleted non-matching rows: {after.height} of 6 left"
    )
    assert result["scoped"] is True


def test_compaction_rejects_unpushable_filter(ice, six_rows):
    """A partially-pushed filter would rewrite too much; refuse instead."""
    table = "default.compact_unpushable"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    with pytest.raises(CompactionError):
        ice.compact_data_files(table, filter_expr=(col("id") > col("v")))


def test_compaction_honours_target_file_size(ice, six_rows):
    """
    ``target_file_size_mb`` was accepted and never used — the primary knob of a
    bin-packing compactor was decorative. It now maps to Iceberg's
    ``write.target-file-size-bytes``.
    """
    table = "default.compact_target_size"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    ice.compact_data_files(table, target_file_size_mb=64)

    props = ice.get_table(table).properties
    assert props.get("write.target-file-size-bytes") == str(64 * 1024 * 1024)


# ---------------------------------------------------------------------------
# 0.2 — AND silently dropped unpushable operands
# ---------------------------------------------------------------------------

def test_and_with_unpushable_operand_is_applied_fully(ice, six_rows):
    """
    Roadmap finding 2. ``And(AlwaysTrue(), X)`` simplifies to ``X``, so the
    "did to_iceberg() return AlwaysTrue?" pushability test was defeated and the
    column-to-column operand was silently dropped. Reproduced: the query
    returned 2 rows where the correct answer is 0 (``id > v`` is never true).
    """
    table = "default.and_pushdown"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    result = ice.query(table).filter((col("id") > col("v")) & (col("g") == "a")).execute()

    assert result.height == 0, (
        f"unpushable AND operand was dropped: got {result.height} rows, expected 0"
    )


def test_and_with_unpushable_operand_keeps_true_matches(ice, six_rows):
    """The residual predicate must not over-filter either."""
    table = "default.and_pushdown_positive"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    # v > id is true for every row; g == "b" selects ids 3 and 4.
    result = ice.query(table).filter((col("v") > col("id")) & (col("g") == "b")).execute()

    assert sorted(result["id"].to_list()) == [3, 4]


def test_or_with_unpushable_operand_is_applied_fully(ice, six_rows):
    """OR was safe by accident; keep it that way."""
    table = "default.or_pushdown"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    result = ice.query(table).filter((col("id") > col("v")) | (col("g") == "c")).execute()

    assert sorted(result["id"].to_list()) == [5, 6]


def test_read_table_applies_unpushable_and_operand(ice, six_rows):
    """The same bug lived in operations.read_table (operations.py:365)."""
    table = "default.read_and_pushdown"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    result = ice.read_table(table, filter_expr=(col("id") > col("v")) & (col("g") == "a"))

    assert result.height == 0


def test_pushdown_reports_partial_pushability():
    """The mechanism itself: pushdown() must report fully=False."""
    expr = (col("id") > col("v")) & (col("g") == "a")
    pushed, fully = expr.pushdown()
    assert fully is False
    # ...and what IS pushed must be a safe superset (the g == "a" half).
    assert "g" in repr(pushed)


def test_not_of_partially_pushed_expression_is_not_pushed():
    """Negating a superset yields a subset — that would drop real rows."""
    expr = ~((col("id") > col("v")) & (col("g") == "a"))
    _pushed, fully = expr.pushdown()
    assert fully is False


def test_simple_predicates_still_push_down():
    """The fix must not disable pushdown for ordinary predicates."""
    pushed, fully = (col("v") > 30).pushdown()
    assert fully is True
    assert "GreaterThan" in type(pushed).__name__


def test_delete_refuses_partially_pushed_filter(ice, six_rows):
    """Deleting on a weaker predicate would remove extra rows."""
    table = "default.delete_unpushable"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    with pytest.raises(ValidationError):
        ice.query(table).filter((col("id") > col("v")) & (col("g") == "a")).delete()

    assert ice.read_table(table).height == 6


# ---------------------------------------------------------------------------
# 0.3 — expire_snapshots called a nonexistent PyIceberg API
# ---------------------------------------------------------------------------

def test_expire_snapshots_actually_expires(ice, six_rows):
    """
    Roadmap finding 3. ``gc.py`` called ``Table.expire_snapshots(...)``, which
    does not exist on PyIceberg's Table; the hasattr guard failed and it raised
    ``NotImplementedError`` whenever it had work to do. The real API is
    ``Table.maintenance.expire_snapshots()``.
    """
    table = "default.expire_me"
    ice.create_table(table, six_rows)
    for _ in range(4):
        ice.append_to_table(table, six_rows)

    before = len(list(ice.get_table(table).snapshots()))
    assert before >= 4

    expired = ice.expire_snapshots(table, older_than_days=0, retain_last=1)

    assert expired, "expire_snapshots returned nothing despite having work to do"
    after = len(list(ice.get_table(table).snapshots()))
    assert after < before, f"no snapshots were expired ({before} -> {after})"
    # Data is untouched.
    assert ice.read_table(table).height == 6 * 4


def test_expire_snapshots_noop_when_nothing_to_do(ice, six_rows):
    table = "default.expire_noop"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    assert ice.expire_snapshots(table, older_than_days=365, retain_last=10) == []


def test_expire_snapshots_retain_last_zero(ice, six_rows):
    """``retain_last=0`` used to silently expire nothing (snapshots[:-0] == [])."""
    table = "default.expire_retain_zero"
    ice.create_table(table, six_rows)
    for _ in range(3):
        ice.append_to_table(table, six_rows)

    expired = ice.expire_snapshots(table, older_than_days=0, retain_last=0)

    # The current snapshot is a protected branch head, so it survives; every
    # other snapshot should go.
    assert len(expired) >= 2
    assert ice.read_table(table).height == 6 * 3


def test_expire_snapshots_rejects_negative_retain(ice, six_rows):
    table = "default.expire_negative"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    with pytest.raises(MaintenanceError):
        ice.expire_snapshots(table, retain_last=-1)


# ---------------------------------------------------------------------------
# 0.5 — create_table(sort_order=[...]) raised AttributeError
# ---------------------------------------------------------------------------

def test_create_table_with_list_sort_order(ice, six_rows):
    """
    Roadmap finding 4. The conversion block was a literal ``pass`` and the raw
    list reached PyIceberg:
    ``AttributeError: 'list' object has no attribute 'is_unsorted'``.
    """
    table = "default.sorted_table"
    ice.create_table(table, six_rows, sort_order=["v"])

    order = ice.get_table(table).sort_order()
    assert order is not None and len(order.fields) == 1

    schema = ice.get_table(table).schema()
    assert schema.find_field(order.fields[0].source_id).name == "v"


def test_create_table_with_descending_sort_order(ice, six_rows):
    table = "default.sorted_desc"
    ice.create_table(table, six_rows, sort_order=[("v", "desc"), "id"])

    from pyiceberg.table.sorting import SortDirection

    order = ice.get_table(table).sort_order()
    assert len(order.fields) == 2
    assert order.fields[0].direction == SortDirection.DESC
    assert order.fields[1].direction == SortDirection.ASC


def test_sort_order_unknown_column_raises_clearly(ice, six_rows):
    with pytest.raises(SchemaError, match="not found in schema"):
        ice.create_table("default.sorted_bad", six_rows, sort_order=["nope"])


def test_sort_order_bad_direction_raises_clearly(ice, six_rows):
    with pytest.raises(SchemaError, match="sort direction"):
        ice.create_table("default.sorted_bad_dir", six_rows, sort_order=[("v", "sideways")])


def test_compaction_applies_sort_order(ice, six_rows):
    """Sort application in compaction was unreachable while list sort_order was dead."""
    table = "default.sorted_compact"
    ice.create_table(table, six_rows, sort_order=[("v", "desc")])
    ice.append_to_table(table, six_rows)

    ice.compact_data_files(table)

    assert ice.read_table(table)["v"].to_list() == [60, 50, 40, 30, 20, 10]


# ---------------------------------------------------------------------------
# 0.4 — null rows passed every DataValidator constraint
# ---------------------------------------------------------------------------

def test_null_rows_fail_constraints():
    """
    Roadmap finding 5. ``df.filter(~expr)`` drops null rows under Polars'
    three-valued logic, so ``validate([{"age": [5, None]}], ["age > 0"])``
    returned ``{"passed": True}`` — a quality gate that passes nulls.
    """
    from iceframe.quality import DataValidator

    result = DataValidator().validate(pl.DataFrame({"age": [5, None]}), ["age > 0"])

    assert result["passed"] is False, "null row passed a value constraint"
    assert result["details"]


def test_null_policy_pass_restores_lenient_behaviour():
    from iceframe.quality import DataValidator

    result = DataValidator().validate(
        pl.DataFrame({"age": [5, None]}), ["age > 0"], null_policy="pass"
    )
    assert result["passed"] is True


def test_null_rows_fail_polars_expression_checks():
    from iceframe.quality import DataValidator

    result = DataValidator().validate(pl.DataFrame({"age": [5, None]}), [pl.col("age") > 0])
    assert result["passed"] is False


def test_null_rows_fail_dict_constraints():
    from iceframe.quality import DataValidator

    for constraint in (
        {"type": "between", "column": "age", "min": 0, "max": 100},
        {"type": "in_set", "column": "age", "values": [5, 7]},
    ):
        result = DataValidator().validate(pl.DataFrame({"age": [5, None]}), [constraint])
        assert result["passed"] is False, f"nulls passed {constraint}"


def test_null_rows_fail_check_constraints():
    from iceframe.quality import DataValidator

    assert DataValidator().check_constraints(pl.DataFrame({"age": [5, None]}), ["age > 0"]) is False


def test_write_validators_block_null_rows(ice):
    """This is the path that gates writes — it must actually gate."""
    table = "default.validated_write"
    ice.create_table(table, pl.DataFrame({"age": [1]}))

    with pytest.raises(ValidationError):
        ice.append_to_table(table, pl.DataFrame({"age": [5, None]}), validators=["age > 0"])

    assert ice.read_table(table).height == 0


def test_clean_data_still_passes():
    from iceframe.quality import DataValidator

    result = DataValidator().validate(pl.DataFrame({"age": [5, 7]}), ["age > 0"])
    assert result["passed"] is True


# ---------------------------------------------------------------------------
# 0.6 — query cache served stale data after writes
# ---------------------------------------------------------------------------

def test_cache_invalidated_after_append(ice, six_rows):
    """
    Roadmap finding 6. ``QueryCache.invalidate(table)`` existed and was
    correct; nothing ever called it, so a cached read kept serving pre-write
    rows for the life of the process.
    """
    from iceframe import query as query_mod
    from iceframe.cache import QueryCache

    query_mod.set_query_cache(QueryCache(max_size=8))

    table = "default.cache_invalidate"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    qb = ice.query(table).cache(ttl=300)
    assert qb.execute().height == 6

    ice.append_to_table(table, six_rows)

    assert qb.execute().height == 12, "cache served stale data after an append"


def test_cache_invalidated_after_overwrite(ice, six_rows):
    from iceframe import query as query_mod
    from iceframe.cache import QueryCache

    query_mod.set_query_cache(QueryCache(max_size=8))

    table = "default.cache_overwrite"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    qb = ice.query(table).cache(ttl=300)
    assert qb.execute().height == 6

    ice.overwrite_table(table, six_rows.head(2))

    assert qb.execute().height == 2


def test_cache_key_includes_snapshot_id(ice, six_rows):
    """Belt and braces: even a missed invalidation can't serve a stale entry."""
    table = "default.cache_snapshot_key"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    qb = ice.query(table).cache(ttl=300)
    snap = ice.get_table(table).current_snapshot()
    params = qb._cache_key_params(snap.snapshot_id)

    assert params["snapshot_id"] == snap.snapshot_id
    assert qb._cache_key_params(1) != qb._cache_key_params(2)


# ---------------------------------------------------------------------------
# 0.7 — silent-failure excepts
# ---------------------------------------------------------------------------

def test_mor_delete_where_propagates_failures(ice, six_rows):
    """``MoRWriter.delete_where`` caught every exception and passed."""
    from iceframe.mor import MoRWriter

    table = "default.mor_delete"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    writer = MoRWriter(ice.get_table(table))
    with pytest.raises(Exception):
        writer.delete_where("this is not a valid predicate at all")


def test_mor_delete_where_works_for_valid_predicate(ice, six_rows):
    from iceframe.mor import MoRWriter

    table = "default.mor_delete_ok"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    MoRWriter(ice.get_table(table)).delete_where("v > 30")
    assert ice.read_table(table).height == 3


def test_mor_delete_file_writers_raise_typed_error(ice, six_rows):
    from iceframe.exceptions import UnsupportedOperationError
    from iceframe.mor import MoRWriter

    table = "default.mor_unsupported"
    ice.create_table(table, six_rows)
    writer = MoRWriter(ice.get_table(table))

    with pytest.raises(UnsupportedOperationError):
        writer.write_position_deletes("f.parquet", [0])
    with pytest.raises(UnsupportedOperationError):
        writer.write_equality_deletes([1], pa.table({"id": [1]}))


def test_no_bare_excepts_in_library():
    """All ten bare ``except:`` clauses swallowed KeyboardInterrupt/SystemExit."""
    import pathlib
    import re

    package = pathlib.Path(__file__).resolve().parent.parent / "iceframe"
    offenders = []
    for path in package.rglob("*.py"):
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if re.match(r"\s*except\s*:", line):
                offenders.append(f"{path.name}:{lineno}")
    assert not offenders, f"bare except clauses found: {offenders}"


def test_no_prints_in_library_code():
    """A library that prints to stdout is unusable inside a pipeline."""
    import pathlib
    import re

    package = pathlib.Path(__file__).resolve().parent.parent / "iceframe"
    allowed = {"cli.py", "agent_cli.py", "magics.py"}
    offenders = []
    for path in package.rglob("*.py"):
        if path.name in allowed:
            continue
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if re.match(r"\s*print\(", line):
                offenders.append(f"{path.name}:{lineno}")
    assert not offenders, f"print() in library code: {offenders}"


# ---------------------------------------------------------------------------
# 2.3 — QueryBuilder pushed neither projection nor limit
# ---------------------------------------------------------------------------

def test_projection_is_pushed_into_the_scan(ice, six_rows):
    table = "default.projection_pushdown"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    qb = ice.query(table).select("id")
    plan = qb._plan_scan(ice.get_table(table))

    assert plan["selected_fields"] == ("id",), plan["selected_fields"]
    assert qb.execute().columns == ["id"]


def test_projection_includes_filter_and_order_columns(ice, six_rows):
    table = "default.projection_full"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    qb = ice.query(table).select("id").filter(col("v") > 30).order_by("g")
    plan = qb._plan_scan(ice.get_table(table))

    assert set(plan["selected_fields"]) == {"id", "v", "g"}
    assert qb.execute().columns == ["id"]


def test_limit_is_pushed_when_safe(ice, six_rows):
    table = "default.limit_pushdown"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    qb = ice.query(table).limit(2)
    assert qb._plan_scan(ice.get_table(table))["limit"] == 2
    assert qb.execute().height == 2


def test_limit_not_pushed_when_ordering_would_change_the_answer(ice, six_rows):
    """Capping before a sort would return the wrong two rows."""
    table = "default.limit_no_pushdown"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    qb = ice.query(table).order_by("v").limit(2)
    assert qb._plan_scan(ice.get_table(table))["limit"] is None

    result = qb.execute()
    assert result["v"].to_list() == [10, 20]


def test_limit_not_pushed_with_residual_filter(ice, six_rows):
    table = "default.limit_residual"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    qb = ice.query(table).filter(col("v") > col("id")).limit(3)
    assert qb._plan_scan(ice.get_table(table))["limit"] is None
    assert qb.execute().height == 3


# ---------------------------------------------------------------------------
# 2.1 / 2.2 — native upsert and transactions
# ---------------------------------------------------------------------------

def test_upsert_updates_and_inserts(ice):
    table = "default.upsert_target"
    base = pl.DataFrame({"id": [1, 2, 3], "v": [10, 20, 30]})
    ice.create_table(table, base)
    ice.append_to_table(table, base)

    result = ice.upsert(
        table, pl.DataFrame({"id": [2, 4], "v": [999, 40]}), join_cols=["id"]
    )

    after = ice.read_table(table).sort("id")
    assert after["id"].to_list() == [1, 2, 3, 4]
    assert after["v"].to_list() == [10, 999, 30, 40]
    assert result["rows_updated"] == 1
    assert result["rows_inserted"] == 1


def test_transaction_commits_atomically(ice, six_rows):
    table = "default.txn_table"
    ice.create_table(table, six_rows)

    before = len(list(ice.get_table(table).snapshots()))

    with ice.transaction(table) as txn:
        txn.set_properties({"owner": "data-eng"})
        txn.append(six_rows.to_arrow())

    reloaded = ice.get_table(table)
    assert reloaded.properties.get("owner") == "data-eng"
    assert ice.read_table(table).height == 6
    assert len(list(reloaded.snapshots())) == before + 1, "expected a single commit"


# ---------------------------------------------------------------------------
# 2.4 — metadata tables
# ---------------------------------------------------------------------------

def test_inspect_metadata_tables(ice, six_rows):
    table = "default.inspect_me"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    inspector = ice.inspect(table)

    snapshots = inspector.snapshots()
    assert isinstance(snapshots, pl.DataFrame) and snapshots.height >= 1

    files = inspector.files()
    assert files.height >= 1

    assert isinstance(inspector.manifests(), pl.DataFrame)
    assert isinstance(inspector.history(), pl.DataFrame)
    assert isinstance(inspector.refs(), pl.DataFrame)
    assert "snapshots" in inspector.available()


def test_count_rows_uses_metadata(ice, six_rows):
    table = "default.count_rows"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)
    assert ice.count_rows(table) == 6


# ---------------------------------------------------------------------------
# 2.7 — orphan-file safety
# ---------------------------------------------------------------------------

def test_remove_orphan_files_defaults_to_dry_run(ice, six_rows):
    import os

    table = "default.orphans"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    location = ice.get_table(table).metadata.location.replace("file://", "")
    orphan = os.path.join(location, "data", "orphan.parquet")
    os.makedirs(os.path.dirname(orphan), exist_ok=True)
    pl.DataFrame({"x": [1]}).write_parquet(orphan)

    found = ice.remove_orphan_files(table, older_than_days=0)

    assert any("orphan.parquet" in p for p in found)
    assert os.path.exists(orphan), "dry run must not delete anything"

    ice.remove_orphan_files(table, older_than_days=0, dry_run=False)
    assert not os.path.exists(orphan)
    assert ice.read_table(table).height == 6


def test_orphan_cleanup_preserves_live_data(ice, six_rows):
    table = "default.orphans_safe"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    ice.remove_orphan_files(table, older_than_days=0, dry_run=False)

    assert ice.read_table(table).height == 6


# ---------------------------------------------------------------------------
# 3.2 — schema type coverage
# ---------------------------------------------------------------------------

def test_dict_schema_maps_extended_types(ice):
    from pyiceberg.types import (
        BinaryType,
        DecimalType,
        IntegerType,
        TimeType,
        UUIDType,
    )

    table = "default.typed_schema"
    ice.create_table(table, {
        "a": "int32",
        "b": "decimal(10, 2)",
        "c": "binary",
        "d": "uuid",
        "e": "time",
    })

    fields = {f.name: f.field_type for f in ice.get_table(table).schema().fields}
    assert isinstance(fields["a"], IntegerType)
    assert isinstance(fields["b"], DecimalType)
    assert fields["b"].precision == 10 and fields["b"].scale == 2
    assert isinstance(fields["c"], BinaryType)
    assert isinstance(fields["d"], UUIDType)
    assert isinstance(fields["e"], TimeType)


def test_dict_schema_supports_required_fields(ice):
    table = "default.required_schema"
    ice.create_table(table, {
        "id": {"type": "long", "required": True},
        "name": "string",
    })

    fields = {f.name: f for f in ice.get_table(table).schema().fields}
    assert fields["id"].required is True
    assert fields["name"].required is False


def test_pyarrow_nested_types_round_trip(ice):
    from pyiceberg.types import ListType, StructType

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("meta", pa.struct([pa.field("k", pa.string()), pa.field("n", pa.int64())])),
    ])

    table = "default.nested_schema"
    ice.create_table(table, schema)

    fields = {f.name: f.field_type for f in ice.get_table(table).schema().fields}
    assert isinstance(fields["tags"], ListType)
    assert isinstance(fields["meta"], StructType)


def test_decimal_round_trips_through_a_write(ice):
    from decimal import Decimal

    table = "default.decimal_write"
    ice.create_table(table, {"id": "long", "amount": "decimal(10, 2)"})
    ice.append_to_table(table, pa.table({
        "id": pa.array([1, 2], pa.int64()),
        "amount": pa.array([Decimal("1.25"), Decimal("9.99")], pa.decimal128(10, 2)),
    }))

    assert ice.read_table(table).height == 2


# ---------------------------------------------------------------------------
# 3.3 — window functions ignored ORDER BY
# ---------------------------------------------------------------------------

def test_row_number_honours_order_by():
    """``RowNumber.to_polars`` had an ``if order_exprs:`` branch whose body was
    ``pass`` — ORDER BY was silently ignored and row numbers were arbitrary."""
    from iceframe.functions import row_number

    df = pl.DataFrame({"g": ["a", "a", "a"], "v": [30, 10, 20]})
    out = df.with_columns(row_number().over(order_by=[col("v")]).to_polars().alias("rn"))

    assert out["rn"].to_list() == [3, 1, 2]


def test_row_number_partitioned_and_ordered():
    from iceframe.functions import row_number

    df = pl.DataFrame({"g": ["a", "a", "b", "b"], "v": [2, 1, 4, 3]})
    out = df.with_columns(
        row_number().over(partition_by=[col("g")], order_by=[col("v")]).to_polars().alias("rn")
    )

    assert out["rn"].to_list() == [2, 1, 2, 1]


def test_rank_uses_every_order_column():
    """``Rank`` used only the first order-by column."""
    from iceframe.functions import rank

    df = pl.DataFrame({"a": [1, 1, 1], "b": [3, 1, 2]})
    out = df.with_columns(rank().over(order_by=[col("a"), col("b")]).to_polars().alias("r"))

    assert out["r"].to_list() == [3, 1, 2]


def test_rank_leaves_gaps_and_dense_rank_does_not():
    from iceframe.functions import dense_rank, rank

    df = pl.DataFrame({"v": [1, 1, 2]})
    out = df.with_columns(
        rank().over(order_by=[col("v")]).to_polars().alias("r"),
        dense_rank().over(order_by=[col("v")]).to_polars().alias("dr"),
    )

    assert out["r"].to_list() == [1, 1, 3]
    assert out["dr"].to_list() == [1, 1, 2]


def test_lead_and_lag():
    from iceframe.functions import lag, lead

    df = pl.DataFrame({"v": [3, 1, 2]})
    out = df.with_columns(
        lead(col("v")).over(order_by=[col("v")]).to_polars().alias("lead"),
        lag(col("v")).over(order_by=[col("v")]).to_polars().alias("lag"),
    )

    # Window order is 1, 2, 3; rows are in original order 3, 1, 2.
    assert out["lead"].to_list() == [None, 2, 3]
    assert out["lag"].to_list() == [2, None, 1]


def test_window_rejects_mixed_sort_directions():
    from iceframe.functions import row_number

    with pytest.raises(ValueError, match="Mixed"):
        row_number().over(order_by=[col("a"), col("b")], descending=[True, False])


def test_aggregates_are_not_pushable():
    """``functions.py`` aggregates returned None from to_iceberg(); ``And(None, ...)``
    would have crashed had one ever reached a filter list."""
    from pyiceberg.expressions import AlwaysTrue

    from iceframe.functions import count
    from iceframe.functions import sum as ice_sum

    for fn in (count(), ice_sum(col("v"))):
        pushed, fully = fn.pushdown()
        assert isinstance(pushed, AlwaysTrue)
        assert fully is False


# ---------------------------------------------------------------------------
# Ergonomics / API completeness
# ---------------------------------------------------------------------------

def test_join_accepts_full_and_maps_outer(ice, six_rows):
    """Polars >= 1.0 renamed "outer" to "full"; only the deprecated spelling
    was accepted and the modern one was rejected outright."""
    table = "default.join_left"
    other = "default.join_right"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)
    right = pl.DataFrame({"id": [1, 2], "extra": ["x", "y"]})
    ice.create_table(other, right)
    ice.append_to_table(other, right)

    assert ice.query(table).join(other, on="id", how="full")._joins[0][2] == "full"
    assert ice.query(table).join(other, on="id", how="outer")._joins[0][2] == "full"

    with pytest.raises(ValidationError):
        ice.query(table).join(other, on="id", how="sideways")


def test_dataframe_ergonomics(ice, six_rows):
    table = "default.ergonomics"
    ice.create_table(table, six_rows)
    ice.append_to_table(table, six_rows)

    assert isinstance(ice.to_arrow(table), pa.Table)
    assert isinstance(ice.lazy(table), pl.LazyFrame)
    assert ice.head(table, 2).height == 2
    assert ice.describe(table).height > 0

    batches = list(ice.scan_batches(table, columns=["id"]))
    assert sum(b.num_rows for b in batches) == 6


def test_list_tables_returns_usable_names(ice, six_rows):
    """``list_tables`` returned ``str(tuple)`` — "('default', 'events')" —
    which could not be fed back into any other IceFrame method."""
    table = "default.listable"
    ice.create_table(table, six_rows)

    names = ice.list_tables("default")
    assert "default.listable" in names
    assert ice.table_exists(names[names.index("default.listable")])


def test_exception_hierarchy_is_backwards_compatible():
    """Existing ``except ValueError`` handlers must keep working."""
    assert issubclass(ValidationError, (IceFrameError, ValueError))
    assert issubclass(SchemaError, (IceFrameError, ValueError))
    assert issubclass(CompactionError, (IceFrameError, RuntimeError))


def test_package_ships_type_information():
    import pathlib

    marker = pathlib.Path(__file__).resolve().parent.parent / "iceframe" / "py.typed"
    assert marker.exists(), "py.typed missing; annotations are invisible downstream"
