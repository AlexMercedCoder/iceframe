# Schema Evolution

IceFrame provides a simple API for evolving table schemas without rewriting data.

## Accessing Schema Evolution

```python
# Get schema evolution interface for a table
schema_evo = ice.alter_table("users")
```

## Smart Schema Evolution (Sync)

You can automatically synchronize your table schema with a DataFrame. This is useful for ETL pipelines where source schemas might change.

```python
new_df = pl.DataFrame({
    "id": [1], 
    "name": ["Alice"],
    "new_col": ["Value"] # New column
})

# Detects 'new_col' and adds it
ice.alter_table("users").sync_schema(new_df)
```

**Features:**
-   **Additive**: Adds new columns automatically.
-   **Type Promotion**: Updates compatible types (e.g., `int` -> `long`).
-   **Safe**: Does not drop columns by default (use `allow_drops=True` to enable).

## Adding Columns

```python
# Add a new column
ice.alter_table("users").add_column("email", "string", doc="User email address")
```

## Dropping Columns

```python
# Drop a column
ice.alter_table("users").drop_column("temp_field")
```

## Renaming Columns

```python
# Rename a column
ice.alter_table("users").rename_column("name", "full_name")
```

## Updating Column Types

Iceberg supports safe type promotion (e.g., int -> long, float -> double).

```python
# Update column type
ice.alter_table("users").update_column_type("age", "long")
```
