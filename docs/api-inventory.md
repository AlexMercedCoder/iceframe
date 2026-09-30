# Public API inventory (toward 1.0)

This is a working document for freezing IceFrame's stable surface before 1.0.
It lists what is public today, proposes a stability tier for each part, and
names the decisions that have to be made first. The tiers are proposals, not
promises: nothing below changes behavior in 0.15.0.

Counts are from 0.15.0: 53 modules, one `IceFrame` facade with 82 public
methods, and 16 names in `iceframe.__all__`.

## Proposed tiers

**Stable at 1.0** (semantic versioning applies; changes need a deprecation cycle):

- `IceFrame`: construction, `read_table`, `scan_batches`, `lazy`, `head`,
  `count_rows`, `describe`, `create_table`, `append_to_table`,
  `overwrite_table`, `delete_from_table`, `upsert`, `transaction`,
  `drop_table`, `table_exists`, `list_tables`, `get_table`, the namespace
  methods, `query`, `inspect`, `alter_table`, `evolve_partition`,
  time travel (`snapshot_id`, `as_of_timestamp`), `rollback_to_snapshot`,
  `rollback_to_timestamp`, `create_branch`, `tag_snapshot`,
  `expire_snapshots`, `remove_orphan_files`, `compact_data_files`,
  `read_incremental`, `add_files`, `register_table`, and the `to_*` exports.
- `QueryBuilder`, `col`, `lit` and the `Expression` classes.
- `MetadataInspector`.
- The exception hierarchy in `iceframe.exceptions`.
- `load_catalog_config_from_env`.

**Beta** (public and supported, but may change in a minor release with a
changelog note): compaction strategies and `z_order_optimize`, garbage
collection, `get_changes` / `get_row_changes`, `AsyncIceFrame`, the CLI,
`query_datafusion`, caching, data quality (`DataValidator`,
`validate_data`), `stats` and `profile_column`, `create_view` / `drop_view`,
the `ingest` readers for formats with real-dependency tests (CSV, JSON,
Parquet, IPC, Avro, ORC, Delta, Lance, Vortex, Excel, SQL, XML, Stata, SPSS).

**Experimental** (no compatibility promise): the agent and MCP server,
`RayExecutor`, Kafka streaming, `CatalogFederation`, `MoRWriter`, the
Pydantic helpers, `Visualizer`, notebook magics, `call_procedure`, and the
readers without real-dependency tests (Hudi, Google Sheets, SAS, Hugging
Face, HTML, clipboard, REST API).

## Decisions needed before 1.0

1. **The 23 `create_table_from_*` methods.** Each is a one-line wrapper that
   reads a format and calls `create_table`. `insert_from_file(format=...)`
   already dispatches by format. Proposal: add
   `create_table_from_file(name, path, format=...)`, deprecate the
   per-format methods in 1.x, and keep only CSV and Parquet as named
   shortcuts.
2. **Three filter parameters on `read_table`.** `filter_expr` (Iceberg
   string or expression), `filter` (IceFrame expression, pushed down) and
   `filter_sql` (Polars SQL, evaluated locally) overlap. Proposal: one
   `filter` that accepts an IceFrame expression or an Iceberg predicate
   string, plus `filter_sql` for local SQL; deprecate `filter_expr`.
3. **Duplicate modules.** `ingest` (format readers) and `ingestion`
   (`DataIngestion`, bulk `add_files`) have confusingly close names;
   `evolution.PartitionEvolution` and `partition.PartitionManager` both
   change partition specs; `maintenance.TableMaintenance`,
   `compaction.CompactionManager` and `gc.GarbageCollector` overlap with each
   other and with facade methods; `parallel.ParallelExecutor`,
   `RayExecutor` and `IceFrame.read_tables_parallel` all read tables in
   parallel. Proposal: the facade methods are the stable API, and the
   manager classes become implementation detail (renamed with a leading
   underscore or documented as internal).
4. **Argument order.** `IceFrame.create_view(name, sql, schema, replace,
   properties, dialect)` and `ViewManager.create_view(name, sql, schema,
   properties, replace, dialect)` differ positionally. Proposal: make every
   argument after the SQL keyword-only at 1.0.
5. **Removals already signalled.** `IceFrame(pool_size=...)` has been
   deprecated and ignored since 0.12.0; remove it at 1.0.
6. **Builtin shadowing.** `iceframe.functions` exports `sum`, `min`, `max`
   and `count`, which shadow Python builtins under `from iceframe.functions
   import *`. Proposal: keep the names but add an `__all__` that excludes
   star-export of builtin names.
7. **`__all__`.** Only 16 names are exported from the package root.
   Anything documented as stable should be importable from `iceframe`
   directly; anything else should not be advertised in the docs as a
   top-level import.

## How to keep this current

Regenerate the symbol list with:

```bash
python - <<'EOF'
import ast, pathlib
for f in sorted(pathlib.Path("iceframe").glob("*.py")):
    tree = ast.parse(f.read_text())
    names = [n.name for n in tree.body
             if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and not n.name.startswith("_")]
    print(f"{f.stem}: {', '.join(names)}")
EOF
```
