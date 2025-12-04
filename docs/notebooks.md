# Notebook Integration

IceFrame provides rich integration with Jupyter Notebooks and IPython environments.

## Installation

To use notebook features, install IceFrame with the `notebook` extra:

```bash
pip install "iceframe[notebook]"
```

## Rich Display

When you display an `IceFrame` instance in a notebook, it shows a formatted summary of the connection and available namespaces.

```python
from iceframe import IceFrame

ice = IceFrame(config)
ice  # Displays HTML summary
```

## Magic Commands

IceFrame includes IPython magic commands to simplify interaction.

### Loading the Extension

First, load the extension:

```python
%load_ext iceframe.magics
```

### %iceframe

Set the active IceFrame instance for magic commands.

```python
ice = IceFrame(config)
%iceframe ice
```

Check status:
```python
%iceframe status
```

### %%iceql

Execute SQL queries directly in a cell using the active IceFrame instance.

```sql
%%iceql
SELECT * FROM my_table LIMIT 10
```

You can also perform joins:

```sql
%%iceql
SELECT 
    t1.name, 
    t2.order_total 
FROM users t1 
JOIN orders t2 ON t1.id = t2.user_id
```

Note: `%%iceql` uses Polars SQL context under the hood. It automatically registers tables referenced in the query from your Iceberg catalog.
