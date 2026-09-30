# IceFrame roadmap

IceFrame is an alpha DataFrame-style layer over PyIceberg. The current priority
is a small, dependable core rather than adding more integrations.

## Release priorities

1. **Core reliability:** keep read, append, overwrite, upsert, transactions,
   metadata inspection, maintenance, and the expression/query planner covered
   by offline contract and regression tests.
2. **Integration verification:** every advertised optional format must have a
   real dependency contract test. Done in 0.15.0 for everything except Hudi,
   Google Sheets, SAS, Hugging Face, HTML, clipboard and REST-API ingestion,
   which stay experimental until they have one.
3. **Catalog verification:** every feature check runs against Apache Polaris
   and the Iceberg REST reference catalog in CI (since 0.15.0), and the
   results are published in `docs/compatibility.md`. Next: an object-store
   warehouse (MinIO) with credential vending, and Lakekeeper.
4. **Upstream tracking:** CI tests the PyIceberg floor (0.11) and, weekly,
   PyIceberg main. Adopt PyIceberg's merge-on-read delete writes when they
   ship (see `docs/merge_on_read.md`).
5. **API stability for 1.0:** decide the open questions in
   `docs/api-inventory.md`, deprecate what is not carried forward, and freeze
   the stable surface.
6. **Performance visibility:** expand `QueryBuilder.explain()`, add repeatable
   scan/upsert/compaction benchmarks, and warn before local full-table work.
7. **Release engineering:** signed and reproducible releases.

## Feature maturity

- **Core:** catalog configuration, CRUD, pushed reads, query expressions,
  upsert, transactions, metadata tables, schema/partition management, export.
- **Beta:** compaction, garbage collection, incremental reads, async facade,
  CLI, DataFusion, caching, quality gates, views, and ingestion from formats
  with real-dependency tests (Delta, Lance, Vortex, Excel, SQL, XML, Stata,
  SPSS, plus the Polars-native formats).
- **Experimental:** agent/MCP integrations, distributed Ray execution,
  Kafka streaming, cross-catalog federation, Pydantic helpers,
  visualization, merge-on-read helpers, and Hudi, Google Sheets, SAS,
  Hugging Face, HTML, clipboard and REST-API ingestion.

Historical audit documents are retained under `docs/archive/`; they describe
the versions named in their filenames and are not statements about current
behavior.
