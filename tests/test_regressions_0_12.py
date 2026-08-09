"""
Regression tests for issues fixed in iceframe 0.12.0.

Every test in this module corresponds to a finding in ``CODE_REVIEW.md`` and
serves as a guard against the specific bug regressing. Each uses a self-contained
SQLite-backed catalog under ``tmp_path`` so it runs offline.
"""

from __future__ import annotations

import warnings

import polars as pl
import pytest

from iceframe import IceFrame, __version__, col, lit
from iceframe.cache import QueryCache

# ---------- shared local-catalog fixtures ----------

@pytest.fixture
def local_ice(tmp_path):
    """A fresh IceFrame backed by a tmp_path SQLite catalog. No double-write,
    no shared state between tests."""
    catalog_db = tmp_path / "catalog.db"
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    config = {
        "uri": f"sqlite:///{catalog_db}",
        "type": "sql",
        "warehouse": f"file://{warehouse}",
    }
    return IceFrame(config)


@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["alice", "bob", "charlie", "dave", "eve"],
        "age": [25, 30, 35, 40, 45],
    })


# ---------- 1.1: no double-write on create_table_from_* ----------

def test_create_table_with_dataframe_schema_does_not_double_write(local_ice, sample_df):
    """Issue 1.1: create_table(schema=df) used to auto-append df, and every
    create_table_from_X helper then appended again -> 2x rows. Now create_table
    must NEVER write data; helpers append once."""
    table_name = "default.no_double_write"
    local_ice.create_table(table_name, sample_df)
    # Pure create -> table is empty
    out = local_ice.read_table(table_name)
    assert out.height == 0, f"create_table must not write data; got {out.height} rows"


def test_create_table_from_parquet_writes_exactly_once(local_ice, sample_df, tmp_path):
    """Issue 1.1 on the actual user-facing path."""
    parquet_path = tmp_path / "src.parquet"
    sample_df.write_parquet(parquet_path)

    table_name = "default.from_parquet_once"
    local_ice.create_table_from_parquet(table_name, str(parquet_path))

    out = local_ice.read_table(table_name)
    assert out.height == sample_df.height, (
        f"create_table_from_parquet wrote {out.height} rows, expected {sample_df.height}"
    )


def test_create_table_from_csv_writes_exactly_once(local_ice, sample_df, tmp_path):
    csv_path = tmp_path / "src.csv"
    sample_df.write_csv(csv_path)
    table_name = "default.from_csv_once"
    local_ice.create_table_from_csv(table_name, str(csv_path))
    assert local_ice.read_table(table_name).height == sample_df.height


# ---------- 1.3: ~/NOT must not silently drop rows ----------

def test_not_of_non_pushable_predicate_keeps_all_rows():
    """Issue 1.3: NotExpression on a non-pushable inner used to return
    Not(AlwaysTrue) == AlwaysFalse, dropping every row. The safe answer is
    AlwaysTrue at pushdown time; NOT is applied locally."""
    from pyiceberg.expressions import AlwaysTrue

    # Force a non-pushable inner by wrapping a literal-on-left compare:
    # BinaryExpression.to_iceberg() short-circuits to AlwaysTrue when the
    # left side isn't a Column.
    not_expr = ~(lit(1) == col("x"))
    pushed = not_expr.to_iceberg()
    assert isinstance(pushed, AlwaysTrue), (
        f"~(non-pushable) must return AlwaysTrue, got {type(pushed).__name__}"
    )


def test_not_of_pushable_predicate_still_pushes_down():
    """Sanity: NOT(col == 1) should push as Not(EqualTo(...))."""
    not_expr = ~(col("x") == 1)
    pushed = not_expr.to_iceberg()
    # Either a real Not(...) or AlwaysTrue (depending on PyIceberg's
    # normalisation), but NEVER AlwaysFalse.
    from pyiceberg.expressions import AlwaysFalse
    assert not isinstance(pushed, AlwaysFalse)


def test_not_via_querybuilder_returns_correct_rows(local_ice, sample_df):
    """End-to-end: ~filter through QueryBuilder must NOT empty out the result."""
    table = "default.not_query"
    local_ice.create_table(table, sample_df)
    local_ice.append_to_table(table, sample_df)

    # Non-pushable filter: a literal-on-left comparison.
    qb = local_ice.query(table).filter(~(lit(99) == col("id")))
    result = qb.execute()
    assert result.height == sample_df.height, (
        f"~filter returned {result.height} rows, expected {sample_df.height}"
    )


# ---------- 1.4: as_of_timestamp time travel ----------

def test_as_of_timestamp_resolves_to_snapshot(local_ice, sample_df):
    """Issue 1.4: as_of_timestamp used to call use_ref(str(ms)) or pass an
    unsupported kwarg. Now it resolves to the snapshot id at or before the ts."""
    import time

    table = "default.time_travel"
    local_ice.create_table(table, sample_df)
    local_ice.append_to_table(table, sample_df)  # snapshot 1

    t1 = local_ice.get_table(table)
    snap1 = t1.current_snapshot()
    assert snap1 is not None
    ts1 = snap1.timestamp_ms

    # second commit
    time.sleep(0.05)  # ensure distinct ms timestamps
    local_ice.append_to_table(table, sample_df)

    # Reading at ts1 should see only the first commit's rows
    df_at_ts1 = local_ice.read_table(table, as_of_timestamp=ts1)
    assert df_at_ts1.height == sample_df.height, (
        f"as_of_timestamp didn't return historical row count: {df_at_ts1.height}"
    )

    # Reading current sees both commits
    df_now = local_ice.read_table(table)
    assert df_now.height == 2 * sample_df.height


def test_as_of_timestamp_before_first_commit_raises(local_ice, sample_df):
    table = "default.tt_before"
    local_ice.create_table(table, sample_df)
    local_ice.append_to_table(table, sample_df)
    with pytest.raises(ValueError, match="No snapshot at or before"):
        local_ice.read_table(table, as_of_timestamp=1)


# ---------- 1.5: limit + local filter ordering ----------

def test_limit_with_local_string_filter_returns_filtered_then_limited(local_ice):
    """Issue 1.5: pushing limit into the scan capped rows BEFORE the local
    string filter ran, so filtered+limited queries returned wrong counts."""
    table = "default.limit_filter"
    df = pl.DataFrame({
        "id": list(range(1, 21)),
        "kind": (["A"] * 10) + (["B"] * 10),
    })
    local_ice.create_table(table, df)
    local_ice.append_to_table(table, df)

    # 10 'B' rows total. Asking for 5 with a 'B' filter must return 5 Bs,
    # not 0-5 mixed As/Bs from a pre-filter cap.
    out = local_ice.read_table(table, filter_expr="kind = 'B'", limit=5)
    assert out.height == 5
    assert set(out["kind"].to_list()) == {"B"}


# ---------- 1.6: QueryBuilder.merge applies column updates ----------

def test_merge_when_matched_update_applies_dict(local_ice):
    """Issue 1.6: matched branch had a bare `pass`; updates were ignored."""
    table = "default.merge_test"
    target = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "score": [10, 20, 30],
    })
    local_ice.create_table(table, target)
    local_ice.append_to_table(table, target)

    source = pl.DataFrame({
        "id": [2, 3, 4],
        "name": ["B2", "C3", "D4"],
        "score": [200, 300, 400],
    })

    local_ice.query(table).merge(
        source,
        on="id",
        when_matched_update={"name": "name", "score": "score"},
        when_not_matched_insert=True,
    )
    out = local_ice.read_table(table).sort("id")
    assert out.height == 4  # id=1 kept, 2 and 3 updated, 4 inserted
    rows = {r["id"]: r for r in out.to_dicts()}
    assert rows[1]["name"] == "a" and rows[1]["score"] == 10
    assert rows[2]["name"] == "B2" and rows[2]["score"] == 200
    assert rows[3]["name"] == "C3" and rows[3]["score"] == 300
    assert rows[4]["name"] == "D4" and rows[4]["score"] == 400


# ---------- 1.7: check_constraints / dict validate ----------

def test_check_constraints_actually_evaluates():
    """Issue 1.7: stub always returned True. Must now reject failing constraints."""
    from iceframe.quality import DataValidator

    df = pl.DataFrame({"age": [10, 20, -1, 30]})
    v = DataValidator()
    assert v.check_constraints(df, ["age >= 0"]) is False
    assert v.check_constraints(df, ["age >= -100"]) is True
    assert v.check_constraints(df, {"non_neg": "age >= 0"}) is False
    assert v.check_constraints(df, [pl.col("age") < 999]) is True


def test_validate_understands_dict_constraints():
    """Issue 1.7: validate(...) used to silently skip non-Expr/callable items."""
    from iceframe.quality import DataValidator
    df = pl.DataFrame({"id": [1, 2, 3, 3], "email": ["a@b", "x@y", "c@d", None]})
    v = DataValidator()
    res = v.validate(df, [
        {"type": "not_null", "column": "email"},  # fails (one null)
        {"type": "unique", "column": "id"},        # fails (3 duplicated)
    ])
    assert res["passed"] is False
    assert len(res["details"]) == 2


# ---------- 1.8: null partition handling ----------

def test_compaction_with_null_partition_does_not_raise(local_ice):
    """Issue 1.8: EqualTo(col, None) is invalid; null partitions used to error."""
    import polars as pl
    table = "default.null_part"
    df = pl.DataFrame({"id": [1, 2, 3], "region": ["us", None, "us"]})
    local_ice.create_table(
        table, df,
        partition_spec=[("region", "identity")],
    )
    local_ice.append_to_table(table, df)
    local_ice.append_to_table(table, df)  # 2 files per partition

    # Should not raise on the None partition key.
    result = local_ice.compact_data_files(table, min_input_files=2)
    assert result.get("rewritten_rows", 0) >= 0


# ---------- 1.2: remove_orphan_files preserves files referenced by older snapshots ----------

def test_remove_orphan_files_keeps_files_referenced_by_older_snapshots(local_ice, sample_df):
    """Issue 1.2: previously only the current snapshot's files were treated as
    referenced — files from older but still-valid snapshots were marked as
    orphans and deleted, breaking time-travel and rollback. Verify that after
    multiple commits and a dry-run orphan scan, no data file from any live
    snapshot is classified as an orphan."""
    from iceframe.gc import GarbageCollector

    table_name = "default.orphan_safety"
    local_ice.create_table(table_name, sample_df)
    local_ice.append_to_table(table_name, sample_df)  # snap A
    local_ice.append_to_table(table_name, sample_df)  # snap B
    local_ice.append_to_table(table_name, sample_df)  # snap C (current)

    table = local_ice.get_table(table_name)

    # Collect the set of files referenced by ALL live snapshots — that is the
    # invariant the fix maintains.
    live_files = set()
    for snap in table.snapshots():
        for m in snap.manifests(table.io):
            for entry in m.fetch_manifest_entry(table.io):
                live_files.add(entry.data_file.file_path)

    gc = GarbageCollector(table)
    orphans = gc.remove_orphan_files(dry_run=True)

    overlap = set(orphans) & live_files
    assert not overlap, (
        f"remove_orphan_files marked live-snapshot files as orphans: {overlap!r}"
    )


# ---------- 1.13: list_tables / table_exists tighter exception surface ----------

def test_list_tables_returns_empty_for_missing_namespace(local_ice):
    """Issue 1.13: previously swallowed all errors as empty list. Missing
    namespaces should still return [], but other errors should surface — we
    only assert the OK path here."""
    assert local_ice.list_tables("does_not_exist") == []


def test_table_exists_false_for_missing(local_ice):
    assert local_ice.table_exists("default.missing_table") is False


# ---------- 1.15 / 3.7: cache invalidate / disk cache hygiene ----------

def test_query_cache_invalidate_only_drops_matching_table():
    """Issue 1.15: invalidate('t1') used to clear the entire cache."""
    cache = QueryCache(max_size=10)
    df1 = pl.DataFrame({"a": [1]})
    df2 = pl.DataFrame({"a": [2]})
    cache.put("t1", {"q": 1}, df1)
    cache.put("t2", {"q": 1}, df2)
    cache.invalidate("t1")
    assert cache.get("t1", {"q": 1}) is None
    assert cache.get("t2", {"q": 1}) is not None


# ---------- 2.1 / 3.1: top-level exports & single-source version ----------

def test_top_level_exports_present():
    """Issue 2.1: col, lit, QueryBuilder, load_catalog_config_from_env were
    not surfaced from the package root."""
    import iceframe
    assert hasattr(iceframe, "col")
    assert hasattr(iceframe, "lit")
    assert hasattr(iceframe, "QueryBuilder")
    assert hasattr(iceframe, "load_catalog_config_from_env")
    assert hasattr(iceframe, "IceFrame")


def test_version_matches_pyproject():
    """Issue 3.1: pyproject said 0.11.1 while __init__ said 0.1.0."""
    assert __version__ != "0.1.0"
    # Must look like a real version, not the editable-fallback sentinel.
    assert "+unknown" not in __version__


# ---------- 2.2: QueryBuilder.cache(ttl) is actually wired up ----------

def test_querybuilder_cache_returns_cached_result(local_ice, sample_df):
    """Issue 2.2: cache(ttl) used to be a no-op. Verify a second execute()
    serves the cached DataFrame without re-scanning."""
    from iceframe import query as query_mod
    # Use a fresh cache so other tests don't interfere.
    query_mod.set_query_cache(QueryCache(max_size=8))

    table = "default.cache_test"
    local_ice.create_table(table, sample_df)
    local_ice.append_to_table(table, sample_df)

    qb = local_ice.query(table).cache(ttl=60)
    first = qb.execute()

    # Re-executing without any write in between must hit the cache.
    second = qb.execute()
    assert second is first, "cache(ttl) did not serve the cached result"

    # 0.13.0: a write MUST invalidate the cache. Previously the cached frame
    # kept being served for the life of the process, so this returned 5.
    local_ice.append_to_table(table, sample_df)
    third = qb.execute()
    assert third.height == 10, (
        f"stale cache after write: expected 10 rows, got {third.height}"
    )


# ---------- 2.4: REST config without token warns instead of erroring ----------

def test_rest_config_without_token_only_warns():
    """Issue 2.4: previously raised, blocking SigV4 / unauthenticated REST."""
    from iceframe.utils import validate_catalog_config
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        validate_catalog_config({"uri": "http://localhost:8181", "type": "rest"})
    assert any("no 'token'" in str(w.message) for w in caught), caught


# ---------- pool deprecation ----------

def test_pool_size_kwarg_is_deprecated(tmp_path):
    """Issue 2.3: the pool was dead weight. The kwarg is kept for back-compat
    but must now emit a DeprecationWarning."""
    config = {
        "uri": f"sqlite:///{tmp_path}/cat.db",
        "type": "sql",
        "warehouse": f"file://{tmp_path}",
    }
    with pytest.warns(DeprecationWarning, match="pool_size"):
        IceFrame(config, pool_size=3)


# ---------- 1.12: unknown type fallback warns ----------

def test_unknown_string_type_warns():
    """Issue 1.12: silent coercion to StringType masked typos like 'int32'."""
    from iceframe.operations import TableOperations
    ops = TableOperations(catalog=None)  # only need the helper
    with pytest.warns(UserWarning, match="Unknown type string"):
        ops._string_to_iceberg_type("flooat")

    # 0.13.0: "int32" is now a recognised alias rather than a silent typo.
    from pyiceberg.types import IntegerType
    assert isinstance(ops._string_to_iceberg_type("int32"), IntegerType)
