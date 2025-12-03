# Query Builder API

IceFrame provides a powerful, fluent Query Builder API for constructing complex queries with SQL-like capabilities.

## Overview

The Query Builder allows you to:
- Select columns and apply expressions
- Filter data with predicate pushdown support
- Group by and aggregate data
- Sort and limit results
- Use window functions and case statements
- Perform write operations (Insert, Update, Delete, Merge)

## Basic Usage

Start a query using `ice.query("table_name")`:

```python
from iceframe.expressions import col, lit

df = (ice.query("sales")
      .select("id", "amount")
      .filter(col("amount") > 100)
      .execute())
```

## Expressions

IceFrame provides a unified expression system that works with both PyIceberg (for pushdown) and Polars (for local processing).

```python
from iceframe.expressions import col, lit

# Binary operations
col("age") > 18
col("status") == "active"

# Boolean logic
(col("age") > 18) & (col("status") == "active")
(col("category") == "A") | (col("category") == "B")

# IN / IS NULL
col("id").is_in([1, 2, 3])
col("name").is_null()
```

## Aggregations

Use standard SQL aggregate functions:

```python
from iceframe.functions import count, sum, avg, min, max

df = (ice.query("sales")
      .select(
          col("region"),
          sum(col("amount")).alias("total_sales"),
          avg(col("amount")).alias("avg_sales")
      )
      .group_by("region")
      .execute())
```

## Window Functions

Support for window functions like `row_number`, `rank`, `dense_rank`:

```python
from iceframe.functions import row_number

df = (ice.query("sales")
      .select(
          col("id"),
          col("amount"),
          row_number().over(
              partition_by=col("region"), 
              order_by=col("amount")
          ).alias("rank")
      )
      .execute())
```

## Case Statements

Conditional logic with `when/otherwise`:

```python
from iceframe.functions import when

df = (ice.query("users")
      .select(
          col("name"),
          when(col("age") < 18, "Minor")
          .when(col("age") < 65, "Adult")
          .otherwise("Senior")
          .alias("age_group")
      )
      .execute())
```

## Write Operations

The Query Builder also supports write operations.

### Insert

```python
ice.query("users").insert(new_data_df)
```

### Update

Update rows matching a filter:

```python
(ice.query("users")
 .filter(col("id") == 123)
 .update({"status": "inactive"}))
```

> [!WARNING]
> Updates are currently implemented as Copy-on-Write (overwrite entire table). Use with caution on large tables.

### Delete

Delete rows matching a filter:

```python
(ice.query("users")
 .filter(col("status") == "deleted")
 .delete())
```

### Merge (Upsert)

Merge source data into the target table:

```python
(ice.query("target_table")
 .merge(
     source_data=source_df,
     on="id",
     when_matched_update={"status": "status", "updated_at": "updated_at"},
     when_not_matched_insert={"id": "id", "status": "status", "created_at": "created_at"}
 ))
```
