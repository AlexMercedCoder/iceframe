# IceFrame Roadmap

**Audit date:** 2026-08-09 · **Version audited:** 0.12.0 (`main` @ `56a85ac`) · **PyIceberg:** 0.11.0 · **Polars:** 1.36.1

---

## 1. Executive summary

IceFrame is a broad, ambitious PyIceberg wrapper: 51 modules / ~8,800 lines of library code, 39 test files / ~3,900 lines, 24 ingestion source formats, a query builder, a CLI, an MCP server, an LLM agent, Jupyter magics, and a maintenance suite. The surface area is genuinely impressive and the 0.12.0 release fixed a real class of data-correctness bugs (double-write on `create_table_from_*`, orphan-file collector deleting live data, `NOT` predicate collapsing to zero rows).

The problem is that **breadth has outrun verification**. The library advertises production maintenance operations that do not work, and its most load-bearing paths — read, write, query builder, schema evolution, stats — are covered by tests that only execute when a live Dremio Cloud REST catalog is reachable with the author's credentials. On this machine, 61 of 179 tests skipped for exactly that reason, and 5 integration tests errored on a 401. Line coverage is **45%**, and the modules with the most dangerous behaviour are the least covered: `gc.py` 49%, `compaction.py` 32%, `query.py` 43%, `operations.py` 55%, `stats.py` / `incremental.py` / `maintenance.py` / `async_ops.py` at **0%**.

Every finding below was reproduced against a local SQLite catalog, not inferred from reading. Four are severe:

| # | Finding | Impact |
|---|---|---|
| 1 | `compact_data_files(filter_expr=...)` on an unpartitioned table **destroys every non-matching row** (`compaction.py:203`, `z_order_optimize` at `:404`). Reproduced: 6 rows → 3. | Silent data loss |
| 2 | A composite filter where one operand can't be pushed down **silently drops that operand** (`query.py:167-182`, `operations.py:362-374`). Reproduced: query returned 2 rows where the correct answer is 0. | Silently wrong results |
| 3 | `expire_snapshots` raises `NotImplementedError` whenever there is anything to expire (`gc.py:58-71`). It only "succeeds" when it has nothing to do. | Advertised maintenance op is non-functional |
| 4 | `create_table(sort_order=["col"])` — the documented list form — raises `AttributeError: 'list' object has no attribute 'is_unsorted'` (`operations.py:288-308`, where the conversion body is a literal `pass`). | Documented API is dead |

Beyond these, the shape of the gap is consistent: **the library reimplements in Polars what PyIceberg 0.11 now does natively, and does it less safely.** `QueryBuilder.merge()` (`query.py:465-570`) hand-rolls an upsert by reading the entire target table and full-overwriting it, while `Table.upsert()` exists. `QueryBuilder.update()` (`query.py:293-462`) hand-rolls partition-scoped overwrite while `Transaction.dynamic_partition_overwrite()` exists. `stats.py` hand-parses snapshot summaries while `Table.inspect` exposes `snapshots`, `files`, `partitions`, `manifests`, `history`, and `refs` as first-class metadata tables. `gc.expire_snapshots` reinvents what `Table.maintenance.expire_snapshots` (with `.older_than()` / `.by_id()` / `.commit()`) already provides.

Operationally the project is pre-production regardless of code quality: **there is no CI** (no `.github/`), no `py.typed`, no logging (25 bare `print()` calls in library code, zero `import logging`), 95 ruff findings including 10 bare `except:` and 8 undefined names, and no local catalog fixture so no contributor can meaningfully run the suite.

The honest positioning: IceFrame is a capable **exploratory / notebook-scale** wrapper with a production-sounding API. The roadmap below is ordered to close that gap — stop the data loss, prove the core with a local catalog and CI, delegate to PyIceberg where PyIceberg has caught up, then grow the surface.

### How this was grounded

| Check | Result |
|---|---|
| `pytest tests/` (py3.11) | 118 passed, 61 skipped, 5 errors, 23s |
| `pytest --cov=iceframe` (unit only) | **45%** line coverage, 3,465 statements, 1,912 missed |
| Skip reason (all 61) | `Catalog at 'https://catalog.dremio.cloud/api/iceberg' is unreachable: RESTError 401` |
| `ruff check iceframe/` | 95 findings: 63 F401, 10 E722, 10 F841, 8 F821, 3 F811, 1 F541 |
| Packaging | `dist/` builds present and correctly untracked; `.env` untracked; 5 stray dev scripts tracked at repo root |
| Behavioural repros | Local SQLite catalog + `file://` warehouse; findings 1–4, 6, 8, 9, 11 reproduced directly |

---

## 2. Findings

### 2.1 Correctness and data safety

**Compaction destroys data when scoped by filter.** `CompactionManager.bin_pack` builds a filtered scan and then calls `self.table.overwrite(arrow_table)` with no `overwrite_filter` (`compaction.py:157`, `:203`). `overwrite()` defaults to `AlwaysTrue`, so the whole table is replaced by the filtered subset. `z_order_optimize` has the identical shape (`compaction.py:393-404`). Reproduced: `compact_data_files("t2", filter_expr="v > 30")` on a 6-row unpartitioned table left 3 rows and reported `{"rewritten_rows": 3, "strategy": "bin_pack_full"}` — success. The partitioned path is correct (it passes `overwrite_filter=part_filter` at `:294`), which is what makes the unpartitioned path so easy to miss.

**Non-pushable operands vanish inside `AND`.** Both filter paths decide pushdown by asking "did `to_iceberg()` return `AlwaysTrue`?" (`query.py:169`, `operations.py:365`). But `BooleanExpression.to_iceberg` (`expressions.py:175-184`) wraps operands in PyIceberg's `And`, and `And(AlwaysTrue(), X)` **simplifies to `X`** — so the composite no longer looks non-pushable, is routed to pushdown only, and the unpushable operand is never applied locally. Reproduced: `(col("id") > col("v")) & (col("g") == "a")` returned 2 rows; `id > v` is never true, so the answer is 0. Any column-to-column comparison ANDed with a simple predicate is wrong. `OR` is safe by accident (`Or(AlwaysTrue, X)` → `AlwaysTrue` → falls back correctly).

**`expire_snapshots` is non-functional.** `gc.py:50-71` builds a `manage_snapshots()` manager, loops over snapshot ids doing literally `pass`, then calls `self.table.expire_snapshots(older_than_ms=..., retain_last=..., delete_func=...)`. PyIceberg 0.11's `Table` has **no** `expire_snapshots` attribute at all — the real API is `Table.maintenance.expire_snapshots` with `.older_than(ts)` / `.by_id(id)` / `.commit()`. The `hasattr` guard fails, control reaches `raise NotImplementedError` at `:67`, and the outer `except Exception` re-raises it as `NotImplementedError: Snapshot expiration not supported: ...`. Reproduced against a table with 5 snapshots. Related: `snapshots[:-retain_last]` (`gc.py:42`) assumes `snapshots()` is chronologically ordered and silently expires nothing when `retain_last=0`; `_parallel_delete` (`:260-273`) leaks a `ThreadPoolExecutor` that is never shut down and returns `None`.

**`sort_order` as a list is dead code.** `operations.py:288-308` detects a list, opens a `try` whose entire body is a comment and `pass`, then assigns the raw list to `create_kwargs["sort_order"]`. Reproduced: `AttributeError: 'list' object has no attribute 'is_unsorted'`. Compaction's sort-application logic (`compaction.py:186-201`) is therefore only reachable for tables whose sort order was set through PyIceberg directly.

**Null rows escape every quality constraint.** `DataValidator` evaluates constraints as `df.filter(~expr).height > 0` (`quality.py:106`, `:144`, `:151`, and every `_check_dict_constraint` branch). Under Polars three-valued logic, `~expr` is null for a null input, the row is filtered out, and the constraint passes. Reproduced: `validate(pl.DataFrame({"age": [5, None]}), ["age > 0"])` → `{"passed": True, "details": []}`. This is exactly backwards for a data-quality gate, and it silently weakens `append_to_table(validators=[...])` (`core.py:846-869`), which is the write-blocking path.

**Query cache serves stale data after writes.** `_QUERY_CACHE` is a process-wide `QueryCache` (`query.py:20`) keyed on table name plus query plan — with no snapshot id and no invalidation hook on `insert`/`update`/`merge`/`delete`. Reproduced: cached query returned 6 rows after an append had taken the table to 7. `QueryCache.invalidate(table)` exists and is correct; nothing calls it.

**Silent-failure patterns.** `MoRWriter.delete_where` (`mor.py:57-63`) catches every exception from `table.delete()` and `pass`es — a delete that fails reports success. `create_table` swallows all namespace-creation errors (`operations.py:194-198`), hiding auth and connectivity failures behind a later, more confusing error. `functions.py` aggregates return `None` from `to_iceberg()`; if one ever reaches a filter list, `And(None, ...)` will crash rather than degrade.

**Orphan-file collection remains risky.** 0.12.0 correctly widened the referenced set to all live snapshots (`gc.py:99-118`), but the valid-metadata set (`:127-147`) covers only metadata JSON, manifest lists, and manifests — **not** Puffin statistics or partition-stats files, which would be classified as orphans and deleted. Separately, the age gate (`:199-236`) can only determine mtime for `file://` paths; on S3/GCS/ADLS `mtime` is always `None`, so every candidate is skipped. Net effect: on object stores the operation does nothing, and on local filesystems it can delete statistics files. `max_workers` is accepted and unused.

### 2.2 API completeness and ergonomics

The gap is best expressed as what PyIceberg 0.11 offers that IceFrame doesn't surface:

| PyIceberg capability | IceFrame status |
|---|---|
| `Table.upsert(df, join_cols=...)` | Not exposed. `QueryBuilder.merge` reimplements it via full-table read + full overwrite (`query.py:465-570`) — O(table) for any upsert, and not atomic. |
| `Transaction` (multi-op atomic commit) | Not exposed at all. Every IceFrame write is its own commit; there is no way to make schema change + append atomic. |
| `Transaction.dynamic_partition_overwrite` | Not exposed. `QueryBuilder.update` hand-rolls per-partition scan + overwrite in a Python loop (`query.py:406-462`). |
| `Table.inspect.{snapshots,files,partitions,manifests,history,refs,entries}` | Not exposed. `stats.py` hand-parses `current_snapshot.summary` instead. |
| `Table.to_polars()` | Not used; IceFrame goes `scan.to_arrow()` → `pl.from_arrow` everywhere. |
| Catalog backends: `glue`, `hive`, `dynamodb`, `bigquery_metastore`, `sql`, `memory` | Work by pass-through, but are undocumented and untested; `validate_catalog_config` only reasons about `rest` (`utils.py:67`). README says "REST catalogs". |
| Positional / equality delete writes | `mor.py` raises `NotImplementedError` for both. Merge-on-read is documented as a module but is a stub. |

Missing ergonomics a DataFrame-shaped wrapper is expected to have: no `to_pandas()` / `to_arrow()` on `IceFrame`; no public lazy handle (`pl.LazyFrame` / `scan_batches` is only on `TableOperations`); no `head()` / `describe()` / `__len__`; no `Transaction` context manager. Two filter dialects coexist without being named — `read_table(filter_expr=...)` treats a string as **Polars SQL** evaluated locally (`operations.py:401`), while `delete_from_table(filter_expr=...)` passes the string to **PyIceberg** as an Iceberg predicate (`operations.py:551`). Same parameter name, different language, opposite pushdown behaviour.

`QueryBuilder` also pushes nothing but the row filter: `execute()` calls `table.scan(row_filter=...)` with no `selected_fields` and no `limit` (`query.py:186`), so `.select("id").limit(10)` on a billion-row table reads the entire table into memory before trimming. Joins are worse — each joined table is read in full (`query.py:196-199`).

Window functions are placeholders. `RowNumber.to_polars` (`functions.py:100-135`) contains an `if order_exprs:` branch whose body is `pass`, so `ORDER BY` is silently ignored and row numbers are arbitrary. `Rank` uses only the first order-by column.

`QueryBuilder.join` validates `how in ["inner","left","right","outer"]` (`query.py:84`). On Polars ≥ 1.0, `"outer"` is deprecated in favour of `"full"` — so the only accepted spelling emits a `DeprecationWarning` and the correct modern spelling is rejected outright.

### 2.3 Test coverage

The headline number (45%) understates the problem, because **the skips are structural, not incidental**. Every one of the 61 skipped tests skips on the same condition: a live Dremio Cloud REST catalog. That means `test_read.py`, `test_update.py`, `test_query_builder.py`, `test_schema.py`, `test_stats.py`, `test_wap_pattern.py` and most of `test_scalability.py` never run for anyone but the author, on any machine, ever — including in whatever passes for release validation. PyIceberg ships `sql` (SQLite) and `memory` catalogs; both work locally, as this audit demonstrated by using one.

Zero-coverage modules that are part of the public API: `stats.py`, `incremental.py`, `maintenance.py`, `async_ops.py`, `mor.py`, `skipping.py`, `federation.py`, `agent_cli.py`, and all three LLM adapters. `test_regressions_0_12.py` (404 lines, 23 tests, all passing) is the model to follow — it pins specific past bugs and runs unconditionally.

No property-based tests, no concurrency tests despite `max_workers` on compaction and a shared mutable `Table` object, no `overwrite_filter` round-trip test (which would have caught finding 1 immediately).

### 2.4 Performance

Beyond missing projection/limit pushdown: `merge()` and the unpartitioned `update()` path materialise the full table in memory. Compaction's partitioned path calls `plan_files()` and then re-scans the same partition (`compaction.py:248-261`), doubling metadata reads. `bin_pack` accepts `target_file_size_mb` and **never uses it** — file sizing is left entirely to PyIceberg's writer config, so the primary knob of a bin-packing compactor is decorative. Parallel compaction holds `commit_lock` around the commit but reads and `refresh()`es the shared `Table` object outside it (`compaction.py:293-305`), so scan planning still races against another worker's metadata mutation. `mcp_server.get_iceframe()` (`mcp_server.py:16-32`) constructs a fresh `IceFrame` — and therefore a fresh catalog connection and auth handshake — on **every** tool call.

### 2.5 Error handling and observability

Zero `import logging` in the package. 25 `print()` calls in library (non-CLI) code, including operational messages like `"Updating N partitions..."` (`query.py:404`), `"Deduplicated: X -> Y rows"` (`compaction.py:183`) and warnings about failed compression settings (`compaction.py:92`). Ten bare `except:` clauses (`compaction.py:165,257,284`, `gc.py:170,208,218,250`, `core.py:861`, `operations.py:304`, `stats.py:108`) swallow `KeyboardInterrupt` and `SystemExit` along with everything else. There is no IceFrame exception hierarchy — callers catch `ValueError`, `NotImplementedError`, `RuntimeError`, and raw PyIceberg exceptions interchangeably with no way to distinguish "your input was wrong" from "the catalog is down".

### 2.6 Docs, dependencies, packaging

Docs are unusually good for the maturity level: 48 files under `docs/` plus 4 runnable recipes. Gaps: `README.md:57` links `docs/views.md` with the literal editorial note `(if exists, or remove)` — and the file doesn't exist. There is no documentation for the four broken/limited behaviours above, no catalog-support matrix, no migration guide from raw PyIceberg, and no performance/scale guidance (nothing tells a reader that `merge()` reads the whole table).

Packaging: `requires-python = ">=3.9"` but classifiers stop at 3.12 and there's no 3.13 entry. `polars>=0.19.0` is unrealistically loose given the code uses `pl.sql_expr`, `how="semi"/"anti"`, and `vertical_relaxed`; meanwhile `pl.count()` (`functions.py:31`) has been deprecated since 0.20.5. `pyiceberg>=0.11.0` has no upper bound despite the library depending on private-ish behaviours. `vortex = ["vortex-data>=0.1.0"]` carries the comment `# Assuming vortex-data based on research`, and `ingest.read_vortex` (`ingest.py:48-83`) documents itself as speculative and unverified. No `py.typed`, so all the type annotations are invisible to downstream users. `Development Status :: 3 - Alpha` at 0.12.0 is at least honest. Five dev scratch scripts are tracked at repo root (`check_sig.py`, `check_txn.py`, `check_txn_inst.py`, `combine_markdown.py`, `combine_markdown_pdf.py`).

### 2.7 Agentic / AI-facing surface

This is a real differentiator and it's further along than most of the library: an MCP server (`mcp_server.py`, 85% covered) and a multi-provider agent (`agent/`, OpenAI + Anthropic + Gemini) with tool definitions. What's missing is what makes an agent surface *safe*: the MCP tools are read-oriented but there's no explicit read-only mode, no dry-run/confirm protocol for anything mutating, no row/byte cap on `execute_query` results, and no structured schema-discovery tool that would let a model plan a query without guessing. The LLM adapters are at 0–36% coverage.

---

## 3. Roadmap

Effort key: **S** ≤ 1 day · **M** 2–5 days · **L** > 1 week.

### Phase 0 — Stop the bleeding (target: 0.12.1, patch)

Nothing else on this list matters if the library can silently delete data.

| # | Item | Description | Rationale | Effort | Depends on |
|---|---|---|---|---|---|
| 0.1 | Fix filtered compaction data loss | Pass `overwrite_filter` to `table.overwrite()` in the unpartitioned `bin_pack` path (`compaction.py:203`) and in `z_order_optimize` (`:404`); when `filter_expr`/`partition_filter` is set, the overwrite must be scoped to it. Add a regression test asserting row count is preserved. | Silent, unrecoverable data loss in a routine maintenance call. | S | — |
| 0.2 | Fix dropped filter operands | Track pushability explicitly instead of inferring it from `AlwaysTrue`. Give `Expression` a `pushable` property (or have `to_iceberg()` return `(expr, fully_pushed: bool)`); when any operand is unpushable, push the safe superset **and** apply the full predicate locally. Fix `query.py:167-182` and `operations.py:362-374` together. | Silently wrong query results — the worst failure mode a query library has. | M | — |
| 0.3 | Fix `expire_snapshots` | Rewrite on `Table.maintenance.expire_snapshots` (`.older_than()` / `.by_id()` / `.commit()`). Sort snapshots by `timestamp_ms` before applying `retain_last`; handle `retain_last=0`; drop the dead `manage_snapshots` loop and the leaking `_parallel_delete`. | A headline maintenance operation that raises whenever it has work to do. | S | — |
| 0.4 | Fix null semantics in quality checks | Constraint failure must be `df.filter(expr.is_null() \| ~expr)` (or `~expr.fill_null(False)`). Apply across `quality.py` and document that nulls fail by default, with an opt-out. | A data-quality gate that passes nulls provides false assurance and gates writes. | S | — |
| 0.5 | Fix or remove list `sort_order` | Build a real `SortOrder` from `List[str]` (resolve field ids, default `IdentityTransform`, ascending) — or raise a clear `TypeError` pointing at `pyiceberg.table.sorting.SortOrder`. Never both accept and break. | Documented API raises `AttributeError` on first use. | S | — |
| 0.6 | Cache correctness | Include the table's current snapshot id in the cache key, and call `QueryCache.invalidate(table)` from every write path. | Stale reads after writes in the same process. | S | — |
| 0.7 | Remove silent-failure `except`s | Fix `mor.delete_where` (`mor.py:57-63`) to propagate; let `create_table` namespace creation distinguish "already exists" from auth/network errors; replace all 10 bare `except:` with typed catches. | Failures that report success are worse than crashes. | S | — |

### Phase 1 — Make it verifiable (target: 0.13.0)

| # | Item | Description | Rationale | Effort | Depends on |
|---|---|---|---|---|---|
| 1.1 | Local catalog fixture | A session-scoped pytest fixture using PyIceberg's `sql` (SQLite + `file://` warehouse) or `memory` catalog. Convert the 61 live-catalog skips to run against it; keep the Dremio path behind an opt-in `--live` marker. | Today nobody but the author can run the tests that cover reading, writing, and querying. Everything downstream depends on this. | M | — |
| 1.2 | CI | GitHub Actions: matrix over 3.9–3.13, `pytest` + coverage gate, `ruff`, `mypy`, `python -m build` + `twine check`. No PyPI publish step. | No CI at all today. Phase 0's fixes need a ratchet or they regress. | S | 1.1 |
| 1.3 | Coverage to 70% | Prioritise `compaction.py` (32%), `query.py` (43%), `gc.py` (49%), `operations.py` (55%), then the 0% public modules: `stats`, `incremental`, `maintenance`, `async_ops`. | 45% coverage concentrated away from the risky code. | L | 1.1 |
| 1.4 | Clear the lint backlog | Fix all 95 ruff findings; the 8 F821s are latent `NameError`s. Add `[tool.ruff.lint]` with an explicit `select` and wire into CI. | Cheap, and undefined names are real bugs. | S | 1.2 |
| 1.5 | Structured logging | Module-level `logging.getLogger(__name__)`; convert all 25 library `print()` calls. Keep `rich` output confined to `cli.py` / `agent_cli.py`. | A library that prints to stdout is unusable inside a pipeline. | S | — |
| 1.6 | Exception hierarchy | `IceFrameError` base with `CatalogError`, `TableNotFoundError`, `SchemaError`, `ValidationError`, `CompactionError`. Wrap PyIceberg exceptions at the boundary. | Callers currently cannot distinguish user error from infrastructure failure. | M | — |
| 1.7 | Packaging hygiene | Add `py.typed`; raise the `polars` floor to `>=1.0` (or gate the deprecated calls); cap `pyiceberg<0.13`; add the 3.13 classifier; remove the 5 tracked root scratch scripts; either verify `vortex-data` against a real release or move `read_vortex` behind an explicit experimental warning. | Type annotations are invisible downstream; dependency floors don't match the code. | S | — |

### Phase 2 — Delegate to PyIceberg, then extend (target: 0.14.0)

| # | Item | Description | Rationale | Effort | Depends on |
|---|---|---|---|---|---|
| 2.1 | `IceFrame.upsert()` on `Table.upsert` | Expose the native upsert; reimplement `QueryBuilder.merge` on top of it, keeping the current Polars path only for cases the native API can't express. | Replaces a full-table read + overwrite with a native, atomic, incremental operation. | M | 1.1 |
| 2.2 | Transaction support | `with ice.transaction("tbl") as txn:` wrapping `Table.transaction()`, so schema change + append + property set commit atomically. | The single largest production-readiness gap: no way to make multiple operations atomic. | M | 1.6 |
| 2.3 | Projection + limit pushdown in `QueryBuilder` | Pass `selected_fields` and `limit` into `table.scan()` when the plan permits (`query.py:186`); push join-side filters too. | `.select().limit()` currently reads the whole table. Largest single perf win. | M | 0.2 |
| 2.4 | Metadata tables via `Table.inspect` | `ice.inspect(tbl).snapshots() / .files() / .partitions() / .manifests() / .history() / .refs()`, returning Polars frames. Rebuild `stats.py` on top. | Replaces hand-parsed summaries with the maintained implementation, and gives users the introspection Spark users expect. | M | — |
| 2.5 | `dynamic_partition_overwrite` for updates | Rewrite `QueryBuilder.update`'s partitioned branch (`query.py:406-462`) on the native API. | Removes a Python-loop rewrite path and its per-partition commit storm. | M | 2.2 |
| 2.6 | Real bin-packing | Honour `target_file_size_mb`: group input files into target-sized bins and set `write.target-file-size-bytes` on the rewrite. Or rename the method and document that sizing is delegated. | The compactor's primary parameter is currently ignored. | M | 0.1, 1.3 |
| 2.7 | Orphan-file safety | Include Puffin/statistics and partition-stats files in the valid set; use `FileIO`-provided metadata for age on object stores instead of `os.stat`; honour `max_workers`; make `dry_run` the default. | Currently deletes statistics files locally and does nothing at all on S3. | M | 1.1 |
| 2.8 | Unify the filter dialect | Name the two languages explicitly — `filter_sql=` (Polars, local) vs `filter=` (Expression/Iceberg, pushed) — deprecate the ambiguous `filter_expr` string, and make `delete_from_table` consistent. | Same parameter name means two different languages with opposite performance. | M | 0.2 |

### Phase 3 — Ergonomics, docs, breadth (target: 0.15.0)

| # | Item | Description | Rationale | Effort | Depends on |
|---|---|---|---|---|---|
| 3.1 | DataFrame ergonomics | `to_pandas()`, `to_arrow()`, `lazy()` returning `pl.LazyFrame`, `head()`, `describe()`, `__len__`. Promote `scan_batches` to the public `IceFrame` API. | A "DataFrame-like library" is missing the DataFrame conveniences. | M | 2.3 |
| 3.2 | Schema type coverage | Extend `_pyarrow_to_iceberg_type` / `_string_to_iceberg_type` (`operations.py:100-162`) to decimal, binary, uuid, time, list, map, struct. Support `required` and nested types in the dict form. | `{"a": "int32", "b": "decimal"}` currently produces two `string` columns (with a warning). Nested data can't round-trip at all. | M | 1.3 |
| 3.3 | Window functions | Implement `ORDER BY` in `RowNumber` (`functions.py:100-135`), multi-column `Rank`/`DenseRank`, and `Lead`/`Lag`. Or mark the module experimental. | Silently ignoring `ORDER BY` produces plausible-looking wrong output. | M | 1.3 |
| 3.4 | Catalog support matrix | Test and document `glue`, `hive`, `sql`, `dynamodb`, `bigquery_metastore` alongside `rest`; extend `validate_catalog_config` beyond its REST-only reasoning. | They already work by pass-through but are undocumented and unverified — users can't tell what's supported. | M | 1.1 |
| 3.5 | Docs truth pass | Fix `README.md:57` (`docs/views.md` + the `(if exists, or remove)` note); document the filter dialects, the memory profile of `merge`/`update`, and what compaction actually does; add a "coming from raw PyIceberg" guide. | Docs currently describe intended behaviour, not shipped behaviour. | M | Phase 0 |
| 3.6 | Async that is actually async | `async_ops.py` wraps sync calls in the default executor and is at 0% coverage. Either give it a real bounded thread pool with tests, or deprecate it and document `asyncio.to_thread`. | Thin sham-async surface with zero tests is worse than none. | S | 1.3 |
| 3.7 | Retire or finish `mor.py` | Merge-on-read delete writes both raise `NotImplementedError`. Either implement position deletes on PyIceberg's writer internals or remove the module and document CoW-only. | A documented module that only raises. | M | 2.2 |

### Phase 4 — Differentiation (1.0 track)

| # | Item | Description | Rationale | Effort | Depends on |
|---|---|---|---|---|---|
| 4.1 | Safe agentic MCP surface | Explicit `--read-only` mode; a dry-run/confirm protocol for every mutating tool; row and byte caps on `execute_query`; a structured `get_schema` tool for query planning; reuse one catalog connection instead of reconnecting per call (`mcp_server.py:16-32`). | The clearest differentiator versus raw PyIceberg — but only if an agent can't silently drop a table. | M | 2.2, 1.6 |
| 4.2 | Maintenance orchestration | `ice.optimize(table)` that inspects file counts, snapshot age, and manifest sizing and runs the right operations in the right order, with a plan/apply split. | The reason to use a wrapper: one call instead of four with correct ordering. | L | 2.6, 2.7 |
| 4.3 | Streaming + CDC | Finish `streaming.py` / `incremental.py` (both thin, `incremental.py` at 0%): exactly-once semantics, checkpointing, a real CDC feed built on `Table.inspect` snapshot diffs. | Incremental reads are a top-three reason teams reach for Iceberg. | L | 2.4, 1.3 |
| 4.4 | Benchmarks | A benchmark suite tracking scan throughput, compaction rate, and upsert cost against raw PyIceberg, run on a schedule in CI. | Perf claims currently rest on nothing; regressions are invisible. | M | 1.2 |
| 4.5 | 1.0 API freeze | Deprecation policy, semantic-versioning commitment, `Development Status :: 5 - Production/Stable`, documented support matrix. | Nothing above is worth much if the API keeps shifting under users. | M | all |

---

## 4. Do next

Ranked by impact per unit of effort. The first five are roughly two weeks of work and change the library's risk profile more than everything after them combined.

| Rank | Item | Effort | Why it's first |
|---|---|---|---|
| 1 | **0.1** Filtered-compaction data loss | S | Highest-severity, smallest fix. One missing `overwrite_filter` argument, twice. Ship as 0.12.1 today. |
| 2 | **0.3** `expire_snapshots` | S | Non-functional headline feature; the correct API (`Table.maintenance.expire_snapshots`) already exists in the pinned PyIceberg. |
| 3 | **0.4** Null semantics in quality checks | S | One-line-per-site fix that turns a false-assurance gate into a real one — and it guards writes. |
| 4 | **1.1** Local catalog fixture | M | Unblocks all testing work and converts 61 permanently-skipped tests into running ones. Every later item is cheaper afterwards. |
| 5 | **0.2** Dropped filter operands | M | Silently wrong results, but the fix touches the expression tree, so it wants the fixture (#4) in place first. |
| 6 | **1.2** CI | S | Trivial once #4 lands; without it, #1–#5 regress. |
| 7 | **0.5 / 0.6 / 0.7** Remaining Phase 0 | S each | Small, independent, each closes a documented-but-broken behaviour. |
| 8 | **2.3** Projection/limit pushdown | M | Biggest performance win available, and the one users will notice first. |
| 9 | **1.5 / 1.7** Logging + packaging | S each | Cheap production-readiness table stakes: no stdout prints, `py.typed`, honest dependency floors. |
| 10 | **2.1 / 2.2** Native upsert + transactions | M each | The largest capability gaps; both replace hand-rolled Polars code with maintained native APIs. |

**Explicitly deferred:** new ingestion formats. At 24 sources the breadth is already the library's strength, and one more reader is worth far less than making the existing read/write/maintain paths trustworthy.
