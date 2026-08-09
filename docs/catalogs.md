# Catalog Support Matrix

IceFrame passes its catalog configuration straight through to
`pyiceberg.catalog.load_catalog`, so **every catalog PyIceberg supports works**.
This page says which ones are actually exercised and what to expect.

| Catalog | `type` | Tested in CI | Views | Notes |
|---|---|---|---|---|
| REST (Dremio, Polaris, Tabular, …) | `rest` | Opt-in (`pytest --live`) | Catalog-dependent | Credential vending supported. The primary production target. |
| SQL / SQLite | `sql` | **Yes — the default test backend** | No | `uri="sqlite:///path/catalog.db"`. Zero-setup local development. |
| SQL / PostgreSQL | `sql` | No | No | Same driver, `uri="postgresql://…"`. Requires `sqlalchemy` + a driver. |
| In-memory | `memory` | No | No | Ephemeral; useful for quick experiments. |
| AWS Glue | `glue` | No | No | Requires `iceframe[aws]`. Works by pass-through. |
| Hive Metastore | `hive` | No | No | Requires `thrift`. Works by pass-through. |
| DynamoDB | `dynamodb` | No | No | Requires `iceframe[aws]`. Works by pass-through. |
| BigQuery Metastore | `bigquery_metastore` | No | No | Works by pass-through. |

"Works by pass-through" means IceFrame does nothing catalog-specific: table
CRUD, reads, writes, compaction and maintenance all go through the PyIceberg
`Catalog`/`Table` interfaces. It is not separately verified by IceFrame's test
suite, so treat it as supported-but-unverified.

## Configuration examples

### REST (with a token)

```python
config = {
    "uri": "https://catalog.dremio.cloud/api/iceberg",
    "type": "rest",
    "warehouse": "my_project",
    "token": "…",
}
```

`validate_catalog_config` warns (it does not raise) when a `rest` catalog has no
`token` or `credential` — unauthenticated and SigV4-signed REST catalogs are
legitimate.

### Local SQLite (no credentials, no network)

```python
config = {
    "uri": "sqlite:////tmp/iceberg/catalog.db",
    "type": "sql",
    "warehouse": "file:///tmp/iceberg/warehouse",
}
```

This is exactly what IceFrame's own test suite uses.

### AWS Glue

```python
config = {"type": "glue", "warehouse": "s3://my-bucket/warehouse"}
```

Install with `pip install "iceframe[aws]"`.

## Feature availability by catalog

Some IceFrame features depend on catalog capabilities rather than on IceFrame:

| Feature | Requirement |
|---|---|
| Views (`create_view` / `drop_view`) | Catalog implements the Iceberg view spec. |
| Branching and tagging | Iceberg v2 tables; supported by `sql`, `rest`, `glue`. |
| `expire_snapshots` | Any catalog — uses `Table.maintenance`, a metadata commit. |
| `remove_orphan_files` | Requires a listable warehouse. Object stores work; file age is read via the FileIO's filesystem. |
| `upsert` / `transaction` | Any catalog — plain PyIceberg commits. |

## Environment variables

`load_catalog_config_from_env()` reads:

| Variable | Maps to |
|---|---|
| `ICEBERG_CATALOG_URI` | `uri` |
| `ICEBERG_CATALOG_TYPE` | `type` (default `rest`) |
| `ICEBERG_WAREHOUSE` | `warehouse` |
| `ICEBERG_TOKEN` | `token` |
| `ICEBERG_CREDENTIAL` | `credential` |
| `ICEBERG_OAUTH2_SERVER_URI` | `oauth2-server-uri` |

See [Environment Variables](variables.md).
