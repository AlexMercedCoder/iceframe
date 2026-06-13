# Changelog

All notable changes to IceFrame are documented in this file.

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
