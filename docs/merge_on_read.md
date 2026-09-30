# Merge-on-read deletes: status and plan

**Short version:** IceFrame does not write merge-on-read delete files, and
will not until PyIceberg does. Deletes, overwrites and upserts are
copy-on-write. Reading tables that already contain position or equality
delete files (written by Spark, Flink, Trino or Dremio) works.

## What was checked (0.15.0 research spike, September 2026)

Against PyIceberg 0.12.0, the latest release:

- `Table.delete()` reads `write.delete.mode`. When it is `merge-on-read`,
  PyIceberg emits "Merge on read is not yet supported, falling back to
  copy-on-write" and rewrites the affected data files. Setting the table
  property does not change what IceFrame or PyIceberg writes.
- PyIceberg defines `DataFileContent.POSITION_DELETES` and
  `EQUALITY_DELETES` and counts them in snapshot summaries, but has no public
  writer for delete files and no public way to commit one. The snapshot
  producers that build manifests (`_FastAppendFiles`, `_DeleteFiles`) are
  private and only write data manifests.
- Format version 3 replaces position delete files with deletion vectors in
  Puffin files. PyIceberg 0.12 does not write those either.

## Options considered

1. **Write delete files inside IceFrame** by producing a Parquet file with
   the `file_path` / `pos` schema and committing it through PyIceberg's
   private snapshot classes. Rejected: it depends on private APIs that change
   between minor releases, it would need a v2 path now and a deletion-vector
   path for v3, and a subtly wrong delete manifest corrupts tables for every
   engine that reads them.
2. **Delegate to an engine** (Spark, Flink, Dremio) when merge-on-read is
   required. This is the recommendation for anyone who needs it today, and
   `MoRWriter`'s error messages say so.
3. **Wait for PyIceberg**, and adopt its API as soon as it ships. Chosen.

## When it lands upstream

The `pyiceberg-main` CI job runs the suite against PyIceberg's main branch
weekly. When a release adds delete-file writes, `MoRWriter.delete_where`
should switch to them for tables with `write.delete.mode=merge-on-read`,
`write_position_deletes` / `write_equality_deletes` should wrap the new API,
and the copy-on-write limitation should come out of the README.
