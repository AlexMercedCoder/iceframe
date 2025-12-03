# Schema Evolution

IceFrame provides a simple API for evolving table schemas without rewriting data.

## Accessing Schema Evolution

```python
# Get schema evolution interface for a table
schema_evo = ice.alter_table("users")
```

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
