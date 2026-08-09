# Changelog

All notable changes to IceFrame are documented in this file.

## 0.13.0 — 2026-08-09

A correctness, verifiability and API-completeness release. Three of the fixes
below are **silent data loss or silently wrong results**; every one of them
ships with a regression test in `tests/test_regressions_0_13.py`.

The other half of this release is that the test suite now runs **offline, for
anyone**. Before 0.13.0, 61 of 179 tests skipped on every machine but the
author's because they required a live Dremio Cloud REST catalog — including
every test of reading, writing, the query builder, schema evolution and stats.

### ⚠️ Behaviour changes you should know about

- **`remove_orphan_files` is now dry-run by default.** It permanently deletes
  files, so deletion is opt-in: pass `dry_run=False`. It also returns the list
  of candidates rather than `None`.
- **Null values now FAIL data-quality constraints.** `DataValidator` treated a
  null as "constraint satisfied" (see below). Validators that silently passed
  before will now correctly fail. Pass `null_policy="pass"` to restore the old
  behaviour per call or per validator.
- **`expire_snapshots` returns the list of expired snapshot ids** instead of
  `None`, and actually expires them.
- **`list_tables` returns `"namespace.table"`** instead of `str(tuple)` —
  literally `"('default', 'events')"`, which could not be passed back into any
  other IceFrame method.
- **`QueryBuilder.join(how="full")` is now accepted**, and `how="outer"` is
  mapped to it. On Polars >= 1.0 `"outer"` is deprecated, so the only spelling
  IceFrame used to accept was the one that emits a `DeprecationWarning`, while
  the correct modern spelling was rejected outright. `"semi"`, `"anti"` and
  `"cross"` are also accepted now.
- **`ORDER BY` is applied before `SELECT`** so you can order by a column you
  did not select, as SQL allows. Previously that raised `ColumnNotFoundError`.
- **Dependency floors match the code.** `polars>=1.0.0` (the code uses
  `pl.sql_expr`, `how="semi"/"anti"`, `vertical_relaxed`, `pl.len()` and
  `rank(descending=...)`), and `pyiceberg` is capped at `<0.13`.
- **Exceptions are typed.** Everything IceFrame raises deliberately now derives
  from `IceFrameError`. Existing `except ValueError:` / `except RuntimeError:` /
  `except NotImplementedError:` handlers keep working — the new classes subclass
  the built-ins they replace.

### Fixed — data loss and wrong results (criticals)

- **Filtered compaction destroyed every non-matching row.**
  `compact_data_files(filter_expr=...)` built a filtered scan and then called
  `table.overwrite(arrow_table)` with **no `overwrite_filter`**. `overwrite`
  defaults to `AlwaysTrue`, so the entire table was replaced by the filtered
  subset. Reproduced: a 6-row table compacted with `"v > 30"` was left with 3
  rows, and the call reported success. `z_order_optimize` had the identical
  shape. Both now scope the overwrite to exactly the predicate the scan was
  scoped to, and a filter that can't be fully pushed to Iceberg is rejected
  rather than applied approximately.
- **`AND` silently dropped operands that couldn't be pushed down.** Both filter
  paths decided pushability by asking "did `to_iceberg()` return `AlwaysTrue`?"
  — but PyIceberg simplifies `And(AlwaysTrue(), X)` to `X`, so the composite no
  longer looked unpushable, was routed to pushdown only, and the unpushable
  operand was never applied anywhere. Reproduced:
  `(col("id") > col("v")) & (col("g") == "a")` returned 2 rows where the correct
  answer is 0. Pushability is now tracked explicitly via
  `Expression.pushdown() -> (expr, fully_pushed)`; a partially-pushed predicate
  pushes a safe superset and re-applies the full predicate locally.
- **Null rows escaped every data-quality constraint.** `DataValidator` computed
  violations as `df.filter(~expr)`. Under Polars' three-valued logic `~expr` is
  *null* for a null input, so the row was filtered out of the violations frame
  and the constraint passed. Reproduced:
  `validate(pl.DataFrame({"age": [5, None]}), ["age > 0"])` returned
  `{"passed": True}`. This also silently weakened
  `append_to_table(validators=[...])`, which gates writes. Violations are now
  `~expr.fill_null(False)`; opt out with `null_policy="pass"`.
- **The query cache served stale data after writes.** `QueryCache.invalidate`
  existed and was correct; nothing called it. Every write path (append,
  overwrite, delete, update, merge, upsert, compaction) now invalidates, and
  cache keys include the table's current snapshot id as a second line of
  defence.
- **`MoRWriter.delete_where` reported success when the delete failed.** It
  caught every exception and `pass`ed. Failures now propagate.

### Fixed — advertised APIs that did not work

- **`expire_snapshots` called a PyIceberg API that does not exist.** `gc.py`
  looked for `Table.expire_snapshots`, which is not an attribute of PyIceberg's
  `Table`; the `hasattr` guard always failed and the method raised
  `NotImplementedError` whenever it had work to do — it only "succeeded" when
  there was nothing to expire. Rewritten on the real API,
  `Table.maintenance.expire_snapshots()` with `.by_id()` / `.commit()`.
  Snapshots are now sorted by `timestamp_ms` before `retain_last` is applied
  (`Table.snapshots()` is not guaranteed chronological), `retain_last=0` works,
  and the leaking `ThreadPoolExecutor` in `_parallel_delete` is gone.
- **`create_table(sort_order=["col"])` raised `AttributeError`.** The documented
  list form hit a conversion block whose entire body was a comment and `pass`,
  then handed the raw list to PyIceberg:
  `'list' object has no attribute 'is_unsorted'`. A real `SortOrder` is now
  built, accepting `"col"`, `("col", "desc")` and
  `("col", "desc", "nulls-last")`. As a consequence compaction's sort-order
  application is reachable for the first time — it was additionally reading
  `SortDirection.is_ascending`, which does not exist, inside a bare `except`
  that swallowed the `AttributeError`.
- **`BranchManager.tag_snapshot` always raised `NotImplementedError`** with the
  working call commented out one line above it. Implemented on
  `ManageSnapshots.create_tag`; `remove_tag` and `list_tags` added.
- **`CompactionManager.sort()` did not exist** despite being documented in the
  README's feature table. Implemented, with the same overwrite scoping as
  `bin_pack`.
- **`DataSkipper.can_skip_file` could never skip anything.** It compared
  `filter_expr.op` against `">"`, `"<"` and `"=="`, but `BinaryExpression`
  stores `"gt"`, `"lt"` and `"eq"`. No branch could match. Fixed and extended
  to `>=` / `<=`.
- **`read_incremental` returned the whole table.** It computed the starting
  snapshot and then ignored it, scanning everything. It now performs a real
  manifest-level incremental scan, reading only the data files added after the
  given snapshot.
- **`target_file_size_mb` was accepted and never used** — the primary knob of a
  bin-packing compactor was decorative. It now sets Iceberg's
  `write.target-file-size-bytes` for the rewrite.
- **Aggregate and window functions returned `None` from `to_iceberg()`.** Had
  one ever reached a filter list, `And(None, ...)` would have crashed. They now
  correctly report themselves as unpushable.

### Added

- **Local SQLite catalog test fixture.** The core suite runs offline against a
  PyIceberg `sql` catalog with a `file://` warehouse — no credentials, no
  network. Live-catalog tests are opt-in via `pytest --live` and marked
  `@pytest.mark.live`.
- **CI** (`.github/workflows/ci.yml`): ruff, pytest with coverage across
  Python 3.9–3.13, mypy, and `python -m build` + `twine check` on every push
  and PR. No publish step; releases are cut manually.
- **`ice.upsert(table, data, join_cols=[...])`** on PyIceberg's native atomic
  `Table.upsert`. `QueryBuilder.merge` routes to it automatically when the merge
  is a plain "update matched, insert unmatched", instead of reading and
  overwriting the entire target table.
- **`with ice.transaction(table) as txn:`** for multi-operation atomic commits —
  previously there was no way to make a schema change and an append atomic.
- **`ice.inspect(table)`** exposing Iceberg metadata tables (`snapshots`,
  `files`, `data_files`, `delete_files`, `partitions`, `manifests`, `history`,
  `refs`, `entries`, `metadata_log_entries`) as Polars DataFrames, via
  `Table.inspect`. `ice.stats()` is rebuilt on top of it.
- **Projection and limit pushdown in `QueryBuilder`.** `.select("id").limit(10)`
  now passes `selected_fields` and `limit` into the Iceberg scan instead of
  reading the whole table and trimming afterwards. Pushdown is skipped whenever
  it would change the answer (ordering, joins, aggregation, or a predicate that
  can't be fully pushed).
- **Partitioned `QueryBuilder.update` uses
  `Transaction.dynamic_partition_overwrite`** — one atomic commit instead of a
  per-partition commit storm — falling back to per-partition overwrites when the
  native path can't express the spec.
- **DataFrame ergonomics on `IceFrame`**: `to_arrow()`, `to_pandas()`,
  `lazy()`, `head()`, `describe()`, `count_rows()` (metadata-based) and a public
  `scan_batches()`.
- **Explicit filter dialects.** `filter=` takes an IceFrame `Expression` and is
  pushed to Iceberg; `filter_sql=` takes a Polars SQL string evaluated locally.
  The ambiguous `filter_expr=` (same name, two languages, opposite pushdown) is
  kept for compatibility but deprecated.
- **Exception hierarchy**: `IceFrameError` with `CatalogError`,
  `TableNotFoundError`, `SchemaError`, `ValidationError`, `CompactionError`,
  `MaintenanceError` and `UnsupportedOperationError`.
- **Extended schema type coverage.** `decimal(p, s)`, `binary`, `uuid`, `time`,
  `timestamptz`, `list<T>`, `map<K, V>` and nested structs, plus common aliases
  (`int32`, `int64`, `float64`, `bool`, `str`). The dict schema form now
  supports `{"type": ..., "required": True}` and inline nested structs. Nested
  field ids are allocated from a single counter so they can't collide.
- **Real window functions.** `RowNumber` honours `ORDER BY` (its ordering branch
  was a literal `pass`, so row numbers were arbitrary); `Rank` and `DenseRank`
  use every order column rather than only the first; `Lead` and `Lag` added.
- **Structured logging** throughout the library — module-level
  `logging.getLogger(__name__)` replacing 25 `print()` calls. `rich` output
  stays confined to the CLIs.
- **`py.typed`**, so IceFrame's annotations are visible to downstream type
  checkers.
- **Safer MCP surface**: read-only by default (`ICEFRAME_MCP_READ_ONLY`), row
  and byte caps on `execute_query` (`ICEFRAME_MCP_MAX_ROWS` /
  `ICEFRAME_MCP_MAX_BYTES`), a structured `get_schema` tool for query planning,
  and a single reused catalog connection instead of a fresh auth handshake on
  every tool call.
- **`AsyncIceFrame` owns a bounded thread pool** instead of hijacking the
  interpreter-wide default executor, accepts an existing `IceFrame`, works as an
  async context manager, and exposes aliases without the `_async` suffix.
- New docs: [`docs/views.md`](docs/views.md) (previously linked from the README
  with the literal note "(if exists, or remove)" — it did not exist),
  [`docs/catalogs.md`](docs/catalogs.md),
  [`docs/transactions.md`](docs/transactions.md),
  [`docs/metadata_tables.md`](docs/metadata_tables.md).

### Improved — safety and observability

- **Orphan-file collection.** Puffin/statistics and partition-stats files are
  included in the valid set (they were classified as orphans and deleted on
  local filesystems); file age is resolved through the FileIO's filesystem on
  object stores instead of `os.stat`, which only ever worked for `file://` and
  made the operation a silent no-op on S3/GCS/ADLS; `max_workers` is honoured;
  files whose age can't be determined are still never deleted.
- **No bare `except:` clauses remain in the library** — all ten swallowed
  `KeyboardInterrupt` and `SystemExit` along with everything else. Namespace
  creation in `create_table` now distinguishes "already exists" from auth and
  network failures instead of hiding them behind a later, more confusing error.
- **Ruff clean.** The 95-finding backlog (including 8 undefined names and a
  missing `import polars as pl` in `schema.py`) is cleared, with an explicit
  `[tool.ruff.lint]` `select` wired into CI.
- Parallel compaction failures now propagate as `CompactionError` instead of
  being logged and swallowed.

### Test coverage

- **242 → 287 passing tests; 61 skips → 6.** Line coverage **45% → 68%**.
- New: `tests/test_regressions_0_13.py` (one test per fix above) and
  `tests/test_coverage_gaps.py` (the modules that shipped at 0% coverage:
  `maintenance`, `skipping`, `federation`, `incremental`, `async_ops`, `mor`,
  plus the MCP surface).

## 0.12.0 — 2026-06-12

This is a bugfix / correctness release driven by an end-to-end code review
of the library. Several long-standing data-correctness issues are fixed and
some dead surface area is removed.

### ⚠️ Behaviour changes you should know about

- **`create_table(schema=df)` no longer auto-appends the DataFrame.** All
  `create_table_from_<source>` helpers (`csv`, `json`, `parquet`, `excel`,
  `sql`, `api`, …) previously wrote every row **twice**: once inside
  `create_table` (silent auto-append) and once via the explicit
  `append_to_table` call that followed. Code calling `create_table_from_*`
  will now produce the correct row count for the first time; code that
  relied on the double-write to compensate elsewhere will need adjusting.
- **`IceFrame(pool_size=...)` is deprecated and ignored.** The old
  `CatalogPool` opened `pool_size` catalog connections eagerly, used exactly
  one for the IceFrame's lifetime, and never released them — costly for
  REST/Glue with no upside. IceFrame now uses a single direct
  `load_catalog(...)` handle. Passing `pool_size` raises a
  `DeprecationWarning`. The `iceframe.pool` module has been removed.
- **`__version__` is now sourced from package metadata** via
  `importlib.metadata.version`, so it always matches `pyproject.toml`.
  Previously `__init__.__version__` said `0.1.0` while pyproject said `0.11.1`.
- **`pandas` is no longer a core dependency.** It is pulled in only when you
  install the extras that actually need it (`xml`, `stats`, `html`,
  `clipboard`).
- **REST catalog config without `token` / `oauth2-server-uri` now warns
  instead of erroring**, so unauthenticated local REST and SigV4 setups
  work without a workaround.
- **Top-level package exports** now include `col`, `lit`, `Expression`,
  `QueryBuilder`, and `load_catalog_config_from_env` alongside `IceFrame`.

### Fixed — data-correctness (criticals)

- **Double-write on every `create_table_from_*` helper.** `TableOperations.create_table`
  no longer auto-appends a DataFrame/Table that was passed as the `schema`
  argument. (`iceframe/operations.py`)
- **`remove_orphan_files` could delete live data.** The referenced-file set
  is now collected across **every live snapshot** rather than only the
  current one, so files needed for `rollback_to_snapshot` and time-travel
  reads are preserved. If any snapshot's manifests can't be read, the
  routine aborts rather than risk deleting referenced data. (`iceframe/gc.py`)
- **`~/NOT` could silently drop every row.** `NotExpression.to_iceberg`
  previously wrapped the inner pushdown sentinel — `Not(AlwaysTrue())` —
  producing `AlwaysFalse` and zero rows. It now falls back to `AlwaysTrue`
  when the inner predicate is non-pushable, and the NOT is applied locally.
  (`iceframe/expressions.py`)

### Fixed — query correctness

- **`as_of_timestamp` time travel works.** Both `read_table` and `scan_batches`
  now resolve a millisecond timestamp to the snapshot id current at that
  time via `table.snapshots()`, instead of (a) calling
  `scan.use_ref(str(ms))` — `use_ref` takes a branch/tag name — or
  (b) passing an unsupported `as_of_timestamp=` kwarg to `scan()`. A
  timestamp before the first commit now raises a clear `ValueError`.
  (`iceframe/operations.py`)
- **`limit` + a local string filter returns the right rows.** When
  `read_table(filter_expr="...", limit=N)` uses a Polars-side filter, the
  scan no longer caps rows *before* the filter is applied. The limit is
  pushed into the scan only when there is no post-scan filter.
  (`iceframe/operations.py`)
- **Non-pushable IceFrame `Expression` filters are now applied locally**
  in `read_table`. Previously they were silently dropped.
- **`QueryBuilder.merge(when_matched_update=...)` applies the column dict.**
  The matched branch used to be a bare `pass`; matched rows are now updated
  per the dict (values can be constants, `pl.Expr`, or names of source
  columns), and the final concat aligns schemas to the target.
  (`iceframe/query.py`)
- **`QueryBuilder.update` on nullable partition columns no longer errors.**
  Null partition values now produce `IsNull(col)` rather than the invalid
  `EqualTo(col, None)`. Same fix applied in the per-partition compactor.
  (`iceframe/query.py`, `iceframe/compaction.py`)

### Fixed — data quality

- **`DataValidator.check_constraints` actually evaluates constraints.** The
  body was previously ~30 lines of design-deliberation comments followed by
  `return True`. It now supports a dict (description → expression), a list
  of Polars SQL strings, or a list of `pl.Expr`, and fails when any row
  violates a constraint.
- **`DataValidator.validate(...)` understands dict-shaped constraints**
  (`{"type": "not_null"|"unique"|"between"|"in_set"|"regex"|"row_count", ...}`)
  in addition to `pl.Expr`, SQL strings, and callables. Previously the
  whole dict was silently skipped.
- **`ice.validator` and `ice.quality` now both bind the IceFrame** so the
  validator can resolve SQL-string inputs through `query_datafusion`.

### Fixed — caching & connection management

- **`QueryBuilder.cache(ttl)` actually caches.** A process-wide `QueryCache`
  is now consulted on `execute()` and populated on completion. Swap it for
  a `DiskCache` via `iceframe.query.set_query_cache(...)`.
- **`QueryCache.invalidate(table_name)` only drops matching entries.** It
  used to clear the entire cache regardless of the argument.
- **`DiskCache` no longer leaks parquet files** under the OS tempdir. It
  now writes payloads under `<cache_dir>/data/<key>.parquet` and deletes
  them on eviction, expiry, overwrite, and `clear()`. It also calls
  `super().__init__`, so inherited methods like `stats()` work.
- **`CatalogPool` removed.** Each IceFrame holds a single direct catalog
  connection; the `pool_size` kwarg is deprecated.

### Fixed — exception handling

- `TableOperations.list_tables` / `table_exists` only swallow
  `NoSuchNamespaceError` / `NoSuchTableError`. Auth and connection errors
  now surface to the caller instead of presenting as an empty list / `False`.
- Schema type fallback (`_pyarrow_to_iceberg_type`,
  `_string_to_iceberg_type`) now emits a `UserWarning` before falling back
  to `StringType`. Previously unmapped types were silently coerced.

### Fixed — concurrency

- Parallel compaction (`max_workers > 1`) now serialises Iceberg commits and
  `refresh()` on the shared Table object via a thread lock. The plan/read
  phases still parallelise. (`iceframe/compaction.py`)

### Fixed — low-priority items

- Unreachable second `return table` in `create_table_from_orc` removed.
- `insert_from_file(format="ndjson")` (and `.jsonl`) now routes to
  `pl.read_ndjson` via a new `iceframe.ingest.read_ndjson` helper. Previously
  it called `pl.read_json` which can't parse newline-delimited JSON.
- `read_folder` skips unknown extensions with a clear message, raises if
  the matched files would span readers that produce incompatible schemas,
  and uses `pl.concat(..., how="vertical_relaxed")` to tolerate harmless
  dtype widening within a single format.

### Removed

- `iceframe/pool.py` (dead `CatalogPool`).
- Large blocks of unreachable / abandoned code in `compaction.py`
  (`bin_pack` partition-stats block, `z_order_optimize` rank columns and
  unused `interleave_bits`).
- Committed test-warehouse artefacts (`test_warehouse*/`, `tmp_catalog_repro/`)
  are now gitignored.
- Ad-hoc `verify_*.py` / `repro_*.py` / `research_*.py` scripts moved out
  of `tests/` into `scripts/manual_verification/`.

### Added

- `iceframe.ingest.read_ndjson` helper for newline-delimited JSON.
- `iceframe.query.set_query_cache` / `get_query_cache` to install a custom
  cache (e.g. `DiskCache`).
- `__version__` resolved from package metadata.
- Regression test module `tests/test_regressions_0_12.py` (23 tests
  covering every fix listed above).

### Test infrastructure

- The `ice_frame` session fixture now skips dependent tests cleanly when
  the configured catalog is unreachable, instead of presenting 70+
  `UnauthorizedError`s as failures.

---

## 0.11.1 — 2026-02-18

No corresponding git commit. Released as a same-day patch over `0.11.0`
with the only differences captured in the uploaded sdist; the source tree
in this repo was not bumped or tagged for this release. Treat as an
unrecorded follow-up to `0.11.0`.

## 0.11.0 — 2026-02-18

No corresponding git commit between the `0.10.0` commit (`a4f0e9b`,
2025-12-11) and the `0.12.0` commit (`c31cf00`, 2026-06-12). The
`0.11.0` and `0.11.1` releases were published to PyPI without their
sources being committed to this repository, so the exact change set
cannot be reconstructed from history. The `0.12.0` release notes above
implicitly cover the long tail of bugs that had accumulated by then.

## 0.10.0 — 2025-12-11

### Changed

- Major expansion of `iceframe/compaction.py` (~370 lines added) covering
  the partition-stats, dry-run / bloom, and parallel-ordering paths.
  Several of those blocks were later identified as dead code and removed
  in 0.12.0. (`a4f0e9b`)

## 0.9.0 — 2025-12-11

No `pyproject.toml` bump landed for this release in git — the commits
below were all tagged `version = "0.8.0"` in source but were what shipped
as `0.9.0` on PyPI.

### Added

- Compaction enhancements across `compaction.py`, `operations.py`,
  `query.py`, and `schema.py` — partition-aware compaction work, the
  initial version of the `bin_pack` / `z_order` planner, and the
  per-partition compactor wired into `QueryBuilder`. (`47892c7`,
  `4b9346a`, `d07c68e`)
- Native maintenance / GC enhancements — `gc.py` and `maintenance.py`
  reworked to use PyIceberg's native APIs where available. (`f258b2d`)

### Fixed

- `create_table` / `read_table` enhancements and bug fixes in
  `operations.py` and `ingest.py`. (`ab93476`)

## 0.8.1 — 2025-12-19

No corresponding git commit. Released as a backport patch over `0.8.0`
roughly a week after `0.10.0`. The exact fix is not recoverable from this
repository.

## 0.8.0 — 2025-12-10

### Added

- Broad ingest-features expansion (`iceframe/ingest.py`, `iceframe/core.py`):
  HTTP API ingest, clipboard ingest, folder ingest, HTML table scraping,
  Hugging Face dataset ingest, and a `variables` document explaining
  configuration. (`a24d676`)

## 0.7.0 — 2025-12-10

### Added

- **DataFusion / lazy / distributed read paths.** New
  `iceframe/datafusion_ops.py`, `iceframe/distributed.py`, plus lazy-read
  and streaming-compaction hooks in `operations.py` / `streaming.py`.
  (`3d49756`)
- **Enhanced data-quality module and a visualization module.** Major
  rework of `iceframe/quality.py`; new `iceframe/visualization.py`.
  (`ce22370`)

## 0.6.0 — 2025-12-09

### Added

- **File-upload ingest paths.** `iceframe/ingest.py` and `core.py` gained
  the "advanced" / "native" / "optional" upload-file feature set
  (uploading files directly into the catalog as an Iceberg table) plus
  documentation under `docs/ingest_advanced.md`, `docs/ingest_native.md`,
  `docs/ingest_optional.md`. (`acc8f08`)

## 0.5.0 — 2025-12-05

### Added

- **MCP server.** New `iceframe/mcp_server.py` exposing IceFrame
  operations over the Model Context Protocol, plus CLI wiring and
  `docs/mcp.md`. (`f1df307`)
- Additional ingest features layered on top of `0.4.0`'s ingest module.
  (`02131c3`)

## 0.4.0 — 2025-12-05

### Added

- **`iceframe.ingest` module.** First-class ingest helpers for CSV,
  JSON, Parquet, Excel, SQL, and friends, with the
  `create_table_from_<source>` helpers on `IceFrame`. Documented in
  `docs/ingest.md`. (`e787be8`)

> Historical note: this is the release that introduced the
> `create_table_from_<source>` → `append_to_table` double-write bug
> later fixed in `0.12.0`.

## 0.3.0 — 2025-12-04

### Added

- **Jupyter notebook magics.** New `iceframe/magics.py` (`%%iceframe` /
  `%iceframe` line magics) with `docs/notebooks.md`. (`d3424c3`)
- **Pydantic integration.** New `iceframe/pydantic.py` for round-tripping
  IceFrame tables with Pydantic models, with `docs/pydantic.md`.
  (`d3424c3`)

### Changed

- Documentation reorganisation — alpha-stage banner across docs, the
  combined-markdown "single-page docs" build, and the
  `docs/combine_markdowns` doc script. (`d1e38df`, `b29a1c0`)

## 0.2.0 — 2025-12-03

### Changed

- Version bump and small documentation-tooling additions
  (`docs/combine_markdowns.py`-style script). No notable behaviour
  changes versus `0.1.0`. (`6a2026b`, `2bee2ea`)

## 0.1.0 — 2025-12-03

Initial public release.

### Added

- **Core IceFrame surface** — `iceframe/__init__.py`, `iceframe/core.py`,
  `iceframe/operations.py`, `iceframe/schema.py`, `iceframe/partition.py`,
  `iceframe/namespace.py`, `iceframe/utils.py`, `iceframe/functions.py`,
  `iceframe/export.py`, `iceframe/maintenance.py`. (`f9f89c1`)
- **`QueryBuilder` and `Expression` system** — `iceframe/query.py`,
  `iceframe/expressions.py`, including the `col(...) / lit(...)` builder
  and predicate pushdown to PyIceberg. (`f9f89c1`)
- **Branching, joins, statistics, async, CLI, incremental, quality** —
  `iceframe/branching.py`, `iceframe/incremental.py`, `iceframe/stats.py`,
  `iceframe/async_ops.py`, `iceframe/cli.py`, `iceframe/quality.py`.
  (`90c6acc`)
- **Advanced lakehouse operations** — `iceframe/mor.py` (merge-on-read),
  `iceframe/gc.py` (orphan-file GC and snapshot expiry),
  `iceframe/evolution.py` (schema evolution), `iceframe/compaction.py`
  (initial bin-pack compaction), `iceframe/procedures.py`. (`9d8f657`)
- **Catalog operations, ingestion, rollback** — `iceframe/catalog_ops.py`,
  `iceframe/ingestion.py`, `iceframe/rollback.py`, additional
  procedures. (`34506b4`)
- **Native maintenance** — `iceframe/gc.py` / `iceframe/compaction.py`
  using PyIceberg's native maintenance APIs where available. (`5f6f07c`)
- **Scalability stack** — `iceframe/cache.py`, `iceframe/federation.py`,
  `iceframe/memory.py`, `iceframe/monitoring.py`, `iceframe/optimizer.py`,
  `iceframe/parallel.py`, `iceframe/pool.py` (later removed in `0.12.0`),
  `iceframe/skipping.py`, `iceframe/streaming.py`. (`9b93208`)
- **AI Agent layer** — `iceframe/agent/` with provider bindings for
  Anthropic (`llm_anthropic.py`), OpenAI (`llm_openai.py`), Gemini
  (`llm_gemini.py`), plus the agent CLI in `iceframe/agent_cli.py`.
  (`9b93208`)
- **Recipes documentation** — `docs/recipes/etl_pipeline.md`,
  `docs/recipes/incremental_ingestion.md`, `docs/recipes/scd_type_2.md`,
  `docs/recipes/data_quality_gate.md`. (`59a08f7`)
