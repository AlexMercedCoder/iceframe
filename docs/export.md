# Exporting Data

IceFrame allows you to export Iceberg table data to common file formats.

## Export to Parquet

Parquet is efficient for analytics and storage.

```python
ice.to_parquet("my_table", "output/data.parquet")
```

With filtering and column selection:

```python
ice.to_parquet(
    "my_table", 
    "output/us_sales.parquet",
    columns=["id", "amount"],
    filter_expr="region = 'US'"
)
```

## Export to CSV

Useful for spreadsheets and simple data exchange.

```python
ice.to_csv("my_table", "output/data.csv")
```

## Export to JSON

Useful for web APIs and NoSQL databases.

```python
ice.to_json("my_table", "output/data.json")
```

## Performance Note

Exports work by reading the data into memory (as a Polars DataFrame) and then writing to disk. For extremely large tables, consider filtering the data first or using a distributed engine.
