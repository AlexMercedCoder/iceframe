# Views

IceFrame exposes a `ViewManager` abstraction over Iceberg views.

> **Catalog support is not universal.** Iceberg views are a catalog-level
> feature. REST catalogs that implement the view spec (Polaris, Tabular,
> Dremio) support them; PyIceberg's `sql` (SQLite) and `memory` catalogs do
> **not**. Calls against a catalog without view support raise an error from the
> catalog itself — IceFrame does not emulate views locally.

## Creating a view

```python
from iceframe import IceFrame, load_catalog_config_from_env

ice = IceFrame(load_catalog_config_from_env())

import pyarrow as pa

ice.create_view(
    "analytics.active_users",
    "SELECT id, name FROM analytics.users WHERE active = true",
    schema=pa.schema([("id", pa.int64()), ("name", pa.string())]),
)
```

The Iceberg view spec stores the result schema next to the SQL, and PyIceberg
does not parse SQL to work it out, so `schema` is required. It accepts a
`pyarrow.Schema`, a PyIceberg `Schema`, or a Polars schema such as
`{"id": pl.Int64, "name": pl.String}`. The SQL is recorded with the `spark`
dialect by default; pass `dialect=` if the engines reading the view expect
another one.

Creating views needs PyIceberg 0.12 or newer. PyIceberg 0.11 can list, load
and drop views but has no way to create them, so `create_view` raises
`UnsupportedOperationError` there.

`replace=True` replaces an existing view instead of failing:

```python
ice.create_view("analytics.active_users", sql, schema=schema, replace=True)
```

## Dropping a view

```python
ice.drop_view("analytics.active_users")
```

## Checking support before you rely on it

Because support varies, guard view usage rather than assuming it:

```python
import polars as pl

from iceframe import IceFrameError

try:
    ice.create_view("analytics.v", "SELECT 1 AS one", schema={"one": pl.Int32})
except IceFrameError as e:
    print(f"This catalog does not support views: {e}")
```

## Alternatives when your catalog has no views

- **DataFusion SQL** (`ice.query_datafusion(...)`) runs SQL locally over one or
  more Iceberg tables without needing catalog view support.
- **The query builder** composes reusable query fragments in Python:

  ```python
  def active_users(ice):
      return ice.query("analytics.users").filter(col("active") == True)  # noqa: E712
  ```

## See also

- [Catalog Support Matrix](catalogs.md)
- [SQL Support (DataFusion)](datafusion.md)
- [Query Builder API](query_builder.md)
