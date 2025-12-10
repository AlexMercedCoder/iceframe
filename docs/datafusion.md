# DataFusion Integration

IceFrame integrates with Apache DataFusion to provide high-performance SQL execution on your Iceberg tables.

## Installation

```bash
pip install "iceframe[datafusion]"
```

## Usage

Use the `query_datafusion` method to execute SQL queries.

```python
# Execute SQL query
df = ice.query_datafusion("SELECT * FROM my_table WHERE id > 100")

# Register multiple tables explicitly if needed (auto-registration is basic)
df = ice.query_datafusion(
    "SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id",
    tables=["table1", "table2"]
)
```

## Performance

DataFusion is an extensible query execution framework written in Rust that uses Apache Arrow as its in-memory format. It is often significantly faster than other engines for complex analytical queries, especially aggregations and joins, due to its vectorized execution engine.
