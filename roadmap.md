# IceFrame roadmap

IceFrame is an alpha DataFrame-style layer over PyIceberg. The current priority
is a small, dependable core rather than adding more integrations.

## Release priorities

1. **Core reliability:** keep read, append, overwrite, upsert, transactions,
   metadata inspection, maintenance, and the expression/query planner covered
   by offline contract and regression tests.
2. **Integration verification:** every advertised optional format must have a
   real dependency contract test. Unsupported or speculative integrations stay
   explicitly experimental.
3. **Performance visibility:** expand `QueryBuilder.explain()`, add repeatable
   scan/upsert/compaction benchmarks, and warn before local full-table work.
4. **Typing and API stability:** keep the complete package at zero mypy errors,
   progressively require annotations on public APIs, document deprecations,
   and freeze the stable surface before 1.0.
5. **Production readiness:** validate REST plus representative cloud catalogs,
   publish a compatibility matrix, and establish signed/reproducible releases.

## Feature maturity

- **Core:** catalog configuration, CRUD, pushed reads, query expressions,
  upsert, transactions, metadata tables, schema/partition management, export.
- **Beta:** compaction, garbage collection, incremental reads, async facade,
  CLI, DataFusion, caching, quality gates.
- **Experimental:** agent/MCP integrations, distributed Ray execution,
  streaming/Kafka, views across catalogs, Vortex/Hudi/Lance ingestion, and
  merge-on-read helpers.

Historical audit documents are retained under `docs/archive/`; they describe
the versions named in their filenames and are not statements about current
behavior.
