# JOIN Support

IceFrame Query Builder supports cross-table joins.

## Basic JOIN

```python
from iceframe.expressions import Column

# Inner join
result = (ice.query("users")
    .join("orders", on="user_id", how="inner")
    .select("name", "order_id", "amount")
    .execute())
```

## JOIN Types

Supported join types: `inner`, `left`, `right`, `outer`

```python
# Left join
result = (ice.query("users")
    .join("orders", on="user_id", how="left")
    .execute())

# Multiple joins
result = (ice.query("users")
    .join("orders", on="user_id")
    .join("products", on="product_id")
    .select("name", "product_name", "amount")
    .execute())
```

## JOIN with Filters

```python
result = (ice.query("users")
    .join("orders", on="user_id")
    .filter(Column("amount") > 100)
    .select("name", "order_id")
    .execute())
```
