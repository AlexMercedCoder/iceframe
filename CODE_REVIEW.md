# IceFrame — Code Review

A read-only assessment of the IceFrame library (`iceframe/`), a PyIceberg + Polars
wrapper that provides a DataFrame-style API over Apache Iceberg tables. Findings are
grouped into **Bugs**, **UX / developer-experience**, and **Other recommendations**,
each ordered by severity. Every item cites a specific file and line range.

---

## 1. Bugs

### Critical

**1.1 — Every `create_table_from_*` helper writes its data twice.**
`TableOperations.create_table` auto-appends the input when the `schema` argument is a
Polars DataFrame (`operations.py:262-270`). All 20+ `create_table_from_X` helpers in
`core.py` pass `schema=df` *and then* call `append_to_table(table_name, df)`
(`core.py:153-154, 176-177, 199-200, 224-225, …`). Net effect: every row ingested
through CSV/JSON/Parquet/Excel/SQL/API/etc. lands in the table twice. This is silent
data duplication on the most common entry points.
*Fix:* either stop auto-appending in `create_table`, or have the helpers call
`create_table(schema=df.schema)` and append once.

**1.2 — `remove_orphan_files` can delete live data and destroy time travel.**
The referenced-file set is built only from the **current** snapshot
(`gc.py:96-101`). Data files belonging to older-but-still-valid snapshots are therefore
classified as orphans (`gc.py:176-220`) and deleted (`gc.py:223-236`). That breaks
`rollback_to_snapshot`, time-travel reads, and any unexpired snapshot. The default
`older_than_days=3` (`core.py:1006`) only narrows the blast radius; older valid files
are still destroyed.
*Fix:* collect referenced files across **all** live snapshots, not just the current one.

**1.3 — Negating a non-pushable predicate silently drops all rows.**
`NotExpression.to_iceberg` returns `Not(self.expr.to_iceberg())` (`expressions.py:204-205`).
When the inner predicate can't be pushed down, `to_iceberg` returns `AlwaysTrue()` as a
"fetch everything, filter locally" sentinel. `Not(AlwaysTrue())` evaluates to
`AlwaysFalse`, so the scan returns **zero rows** — and because `read_table`/`execute`
treat the result as the pushdown filter (`operations.py:316-323`, `query.py:120-135`),
the data is dropped with no error. A semantically benign `~(...)` becomes a data-loss bug.
*Fix:* if the inner expression is non-pushable, the whole `Not` must fall back to
`AlwaysTrue` + local evaluation, not invert the sentinel.

### Medium

**1.4 — `as_of_timestamp` time travel is broken, two different ways.**
`read_table` calls `scan.use_ref(str(as_of_timestamp))` (`operations.py:334-335`), but
`use_ref` expects a branch/tag **name**, not a millisecond epoch — it will raise or
mis-resolve. `scan_batches` instead passes `as_of_timestamp=` as a `table.scan()` kwarg
(`operations.py:391-392`), which PyIceberg's `scan()` does not accept. Neither path
performs timestamp-based time travel correctly, and they disagree with each other.
*Fix:* resolve the timestamp to a snapshot id (`table.history()` / metadata log) and
pass `snapshot_id=`.

**1.5 — `limit` pushdown combined with a string filter returns wrong results.**
`read_table` pushes `limit` into the scan (`operations.py:328`) but applies the local
string filter *after* `to_arrow()` (`operations.py:342-343`). The scan caps rows
**before** the filter runs, so a `filter_expr` + `limit` query can return fewer than
`limit` matching rows, or miss matches that sit past the scan cap.
*Fix:* don't push `limit` when a post-scan local filter is present (or push the filter too).

**1.6 — `QueryBuilder.merge` ignores `when_matched_update`.**
The matched branch builds `df_update` and then has a bare `pass` exactly where the
column updates should be applied (`query.py:466-469`). The `when_matched_update` dict
values are never used; matched rows are replaced wholesale by the source row. The final
`pl.concat([df_keep, df_update, df_insert])` (`query.py:484`) also assumes identical
schemas/column order across source and target and will misalign or raise otherwise.
*Fix:* apply the update dict to matched rows; align schemas before concat.

**1.7 — `check_constraints` is a stub that always passes.**
`DataValidator.check_constraints` returns `True` unconditionally (`quality.py:107`) after
~30 lines of design-deliberation comments (`quality.py:72-106`). Any constraint "passes."
Relatedly, `core.validate_data` forwards dict constraints (`core.py:1313-1328`) to
`DataValidator.validate`, which only understands `pl.Expr` / callables and silently skips
everything else (`quality.py:122-138`). Both give a false sense of safety in a
data-quality gate.

**1.8 — Null partition values break update and compaction.**
`QueryBuilder.update` (`query.py:376-385`) and `CompactionManager`
(`compaction.py:318-323`, `305-311`) construct `EqualTo(col, val)` from distinct
partition values. A null partition produces `EqualTo(col, None)`, which is invalid —
`IsNull` is required. Tables with nullable partition columns will error or skip.

**1.9 — Parallel compaction is not commit-safe.**
With `max_workers > 1`, `process_partition` calls `self.table.overwrite(...)` and
`self.table.refresh()` on the **shared** table object from multiple threads
(`compaction.py:372, 388, 395-403`). Iceberg commits and `refresh()` against one Table
instance race; the retry/back-off loop (`compaction.py:369-388`) masks the symptom but
the concurrency model is unsound.

### Low

- **1.10** Unreachable `return table` after an earlier `return` in
  `create_table_from_orc` (`core.py:414-416`).
- **1.11** `insert_from_file` maps `ndjson` to `read_json`, which calls `pl.read_json`
  rather than `pl.read_ndjson` (`core.py:683-684`, `ingest.py:174-185`) — newline-delimited
  input will fail to parse.
- **1.12** `_pyarrow_to_iceberg_type` / `_string_to_iceberg_type` silently coerce any
  unmapped type (decimal, list, struct, binary, time, …) to `StringType`
  (`operations.py:94-96, 110`) — schema corruption with no warning.
- **1.13** `list_tables` and `table_exists` swallow *all* exceptions
  (`operations.py:491-495, 499-503`), reporting connection/permission failures as an
  empty list / `False`.
- **1.14** `read_folder` forwards the same `**kwargs` to every reader type and relies on
  `pl.concat` with identical schemas (`ingest.py:447-465`); mixed-type folders or
  schema drift will raise.
- **1.15** `QueryCache.invalidate(table_name)` ignores its argument and clears the entire
  cache (`cache.py:92-101`).

---

## 2. UX / Developer-Experience

**2.1 — The fluent API the docs advertise isn't importable from the top level.**
`__init__.py` exports only `IceFrame` (`__init__.py:10-12`). Users must reach into
`iceframe.expressions` for `col`/`lit` and `iceframe.query` for `QueryBuilder`.
`load_catalog_config_from_env` is also unexported. Surface `col`, `lit`, and the helper
in `__all__`.

**2.2 — Query caching is presented but does nothing.**
`QueryBuilder.cache(ttl)` only stores `_cache_ttl` (`query.py:100-111`); `execute()`
never reads or writes a cache, and `QueryCache`/`DiskCache` (`cache.py`) are imported
nowhere. The whole caching feature is dead end-to-end.

**2.3 — The connection pool is dead weight.**
`core` pulls exactly one connection at init and holds it for the object's lifetime
(`core.py:56-57`); `return_connection`/`close_all` are never called (verified across the
package). Meanwhile `_initialize_pool` eagerly opens `pool_size` (default 5) catalog
connections at startup (`pool.py:36-42`) — costly for REST/Glue — leaving four idle.
Either wire acquire/return into each operation or remove the pool; the `pool_size`
parameter currently misleads.

**2.4 — REST validation rejects valid configs.**
`validate_catalog_config` requires every `type="rest"` catalog to carry `token` or
`oauth2-server-uri` (`utils.py:60-64`). Unauthenticated/local REST catalogs and SigV4
setups are legitimate and would be blocked. Loosen to a warning, or check at connect time.

**2.5 — Two filter languages, undocumented.**
`read_table(filter_expr=...)` treats a string as a **local** Polars filter
(`operations.py:320-322, 342-343`), while `QueryBuilder.filter` uses the Expression DSL
with **pushdown** (`query.py:120-135`). Same concept, different semantics and performance,
with no guidance on which to use.

**2.6 — Misleading method names.**
`z_order_optimize` performs a plain hierarchical `df.sort(columns)` and even prints a
runtime warning saying so (`compaction.py:583-584`). `configure_bloom_filters` and the
`compression` option only set table properties affecting **future** writes
(`compaction.py:424-449, 174-181`), not existing files — "enable/optimize" implies action
on current data.

**2.7 — Inconsistent accessor wiring.**
`ice.quality` constructs `DataValidator(self)` (`core.py:762-771`) but `ice.validator`
constructs `DataValidator()` with no IceFrame (`core.py:1240-1244`), so the latter can't
resolve SQL-string inputs. Pick one.

**2.8 — Stream-of-consciousness comments shipped in the source.**
Design-deliberation prose remains in docstrings and bodies — `quality.py:72-107`,
`compaction.py:149-160` and `500-583`, `query.py:299-335` — and surfaces in `help()`.
Replace with crisp docstrings.

---

## 3. Other Recommendations

**3.1 — Version is declared in two places and they disagree.**
`__init__.__version__ = "0.1.0"` (`__init__.py:8`) vs `version = "0.11.1"`
(`pyproject.toml:7`). Use a single source (e.g. `importlib.metadata.version`).

**3.2 — `pandas` is a hard core dependency for optional features.**
`pandas>=2.0.0` sits in core `dependencies` (`pyproject.toml:33`) but is only used by the
xml/sas/spss/stata/clipboard/html readers (`ingest.py:268-423`). Move it to the relevant
extras; it's a heavy import most users won't need.

**3.3 — Large blocks of dead code in `compaction.py`.**
`bin_pack` gathers `partition_stats` and builds `partitions_to_compact`
(`compaction.py:41-160`), then the loop at `129-160` only `pass`es and the result is
discarded — the real implementation restarts at `238`, re-scanning the table. `z_order_optimize`
carries an unused `interleave_bits` function (`compaction.py:511-537`), unused rank columns
(`490-495`), and an unused `z_col_name`. This doubles I/O and makes the module very hard to
follow; delete the abandoned branches.

**3.4 — Test artifacts are committed to the repo.**
`test_warehouse*/catalog.db` plus many parquet/avro/metadata files and `.pytest_cache`
are git-tracked. Add them to `.gitignore` and generate warehouses via `tmp_path` fixtures.
(Note: `.env` itself is correctly gitignored — only `.env.example` is tracked, which is right.)

**3.5 — No projection or filter pushdown in `QueryBuilder.execute`.**
It scans all columns then selects locally (`query.py:138-198`), and joins read the entire
other table with no projection (`query.py:149-152`). Push `selected_fields` and the combined
row filter into the scan for large tables.

**3.6 — Broad exception swallowing and `print`-based diagnostics.**
`except Exception: pass` / bare `except:` recur throughout (`operations.py:142-146, 271-272`;
`compaction.py:90-92, 254, 338, 365`; `gc.py:240-241` re-raises everything as
`NotImplementedError`). Combined with `print(...)` for warnings, this hides real errors and
is hard to silence. Adopt the `logging` module and narrow the caught types.

**3.7 — `DiskCache` leaks files and has broken inherited methods.**
It writes parquet to a system `NamedTemporaryFile(delete=False)` and never deletes old
files on eviction/expiry/overwrite (`cache.py:167-185`) — an unbounded disk leak — and
ignores the configured `cache_dir` for the actual data. It also skips `super().__init__`,
so inherited `invalidate`/`stats` reference a missing `self._cache` (`cache.py:123-141`).
`QueryCache` is also unsynchronized despite the library's parallel/async story.

**3.8 — Deprecated asyncio usage.**
`asyncio.get_event_loop()` inside coroutines (`async_ops.py:40, 58, 86, 117`) is deprecated
on 3.10+. Use `asyncio.get_running_loop()`.

**3.9 — Test layout is noisy.**
`verify_*.py`, `repro_*.py`, `research_*.py`, and a root-level `test_catalog.py` are mixed
into the tree, and there appear to be no regression tests for the bugs above (double-write,
orphan-file safety, `NOT` pushdown, null partitions). Add targeted negative-path tests and
consolidate the ad-hoc scripts.

---

## Priority shortlist

1. **1.1 double-write** — corrupts data on the primary ingestion path.
2. **1.2 orphan-file deletion** — can delete live data / break time travel.
3. **1.3 `NOT` pushdown** — silently returns zero rows.
4. **1.7 stub validator** — data-quality gate that never fails.
5. **1.4 / 1.5 time-travel & limit+filter** — wrong query results.

Items 2.2 (dead caching) and 2.3 (dead pool) are not data-correctness issues but are
prominent advertised features that don't work, so they're worth resolving early for
credibility.
