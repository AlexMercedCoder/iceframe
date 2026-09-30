# DataFusion Integration

IceFrame integrates with Apache DataFusion to provide high-performance SQL execution on your Iceberg tables.

## Installation

```bash
pip install "iceframe[datafusion]"
```

DataFusion 48 or later is required.

## Usage

```python
# Tables named after FROM or JOIN are registered automatically
df = ice.query_datafusion(
    "SELECT o.id, u.name FROM sales.orders o JOIN crm.users u ON o.user_id = u.id"
)

# Or name the tables yourself, for example when detection is not enough
df = ice.query_datafusion("SELECT count(*) FROM orders", tables=["sales.orders"])
```

Each table is registered under a DataFusion schema named after its
namespace, so `sales.orders` resolves exactly as it does in IceFrame. It is
also reachable by its bare name (`orders`) unless another registered table
already took that name.

For repeated queries over the same tables, keep a manager so the tables are
scanned once:

```python
from iceframe.datafusion_ops import DataFusionManager

dfm = DataFusionManager(ice)
dfm.register_table("sales.orders")
dfm.register_table("crm.users", alias="u")
dfm.query("SELECT count(*) FROM sales.orders")
dfm.query("SELECT count(*) FROM u")
```

## How tables reach DataFusion

IceFrame scans each registered table into memory as Arrow and hands
DataFusion an Arrow dataset. DataFusion does not read the Iceberg files
itself, so filters in your SQL do not reduce what is scanned. For large
tables, filter with the query builder first or register a narrower view of
the data.

Before 0.15.0 this integration failed on every query with current DataFusion
releases and could not resolve namespace-qualified names.

## Performance

DataFusion is an extensible query execution framework written in Rust that uses Apache Arrow as its in-memory format. It is often significantly faster than other engines for complex analytical queries, especially aggregations and joins, due to its vectorized execution engine.
