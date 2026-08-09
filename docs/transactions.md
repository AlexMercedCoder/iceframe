# Transactions & Upserts

Added in **0.13.0**. Both delegate to PyIceberg's native implementations rather
than reimplementing them in Polars.

## Upsert (MERGE)

`ice.upsert()` wraps `Table.upsert`. It rewrites only the data files that
actually contain matching rows and commits once.

```python
import polars as pl

new_rows = pl.DataFrame({"id": [2, 4], "v": [999, 40]})

result = ice.upsert("db.users", new_rows, join_cols=["id"])
# {'rows_updated': 1, 'rows_inserted': 1}
```

Arguments:

| Argument | Meaning |
|---|---|
| `join_cols` | Match key. Defaults to the table's identifier fields. |
| `when_matched_update_all` | Update matched rows from the source (default `True`). |
| `when_not_matched_insert_all` | Insert unmatched source rows (default `True`). |

### Upsert vs `QueryBuilder.merge`

| | `ice.upsert()` | `QueryBuilder.merge()` |
|---|---|---|
| Rows read | Only files containing matches | **The entire target table** |
| Commits | One, atomic | One overwrite of the whole table |
| Update rules | Replace matched rows wholesale | Per-column expressions, source-column references |

`QueryBuilder.merge` automatically routes to the native upsert when the merge is
a plain "update matched, insert unmatched" — you only pay the full-table cost
when you actually need column-level update rules.

```python
# Native fast path.
ice.query("db.users").merge(source, on="id", when_matched_update=True,
                            when_not_matched_insert=True)

# Copy-on-write path (reads the whole table) — needed for column-level rules.
ice.query("db.users").merge(
    source, on="id",
    when_matched_update={"v": "v", "updated_at": pl.lit(datetime.now())},
    when_not_matched_insert=True,
)
```

## Transactions

`ice.transaction(table)` returns PyIceberg's `Transaction` context manager.
Everything staged inside the block lands in a **single commit**, producing one
snapshot instead of one per operation.

```python
with ice.transaction("db.events") as txn:
    txn.set_properties({"owner": "data-eng"})
    txn.append(arrow_table)
```

Available operations on `txn` include `append`, `overwrite`, `delete`,
`upsert`, `dynamic_partition_overwrite`, `add_files`, `set_properties`,
`remove_properties`, `update_schema`, `update_spec`, `update_sort_order`,
`update_location` and `update_snapshot`.

Nothing is visible to other readers until the block exits; an exception inside
the block aborts the whole transaction.

### Why this matters

Before 0.13.0 every IceFrame write was its own commit, so there was no way to
make a schema change and an append atomic. A reader could observe the new schema
with none of the new data.

## Partitioned updates

`QueryBuilder.update()` on a partitioned table now uses
`Transaction.dynamic_partition_overwrite`, replacing every affected partition in
one atomic commit instead of issuing one commit per partition:

```python
ice.query("db.events").filter(col("region") == "eu").update({"status": "archived"})
```

If the installed PyIceberg or the table's partition spec can't express a dynamic
overwrite, IceFrame falls back to per-partition `overwrite(..., overwrite_filter=…)`
and logs a warning.

## Cache invalidation

Every write path — append, overwrite, delete, update, merge, upsert and
compaction — invalidates the process-wide query cache for that table. Cache
entries are also keyed on the table's current snapshot id, so a stale entry
cannot be served even if invalidation were missed.

## See also

- [Metadata Tables](metadata_tables.md)
- [Query Builder API](query_builder.md)
- [Scalable Updates](scalable_updates.md)
