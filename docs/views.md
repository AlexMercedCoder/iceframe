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

ice.create_view(
    "analytics.active_users",
    "SELECT id, name FROM analytics.users WHERE active = true",
)
```

`replace=True` replaces an existing view instead of failing:

```python
ice.create_view("analytics.active_users", sql, replace=True)
```

## Dropping a view

```python
ice.drop_view("analytics.active_users")
```

## Checking support before you rely on it

Because support varies, guard view usage rather than assuming it:

```python
from iceframe import IceFrameError

try:
    ice.create_view("analytics.v", "SELECT 1")
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
