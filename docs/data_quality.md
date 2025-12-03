# Data Quality

IceFrame includes a Data Validator to ensure data quality before or after operations.

## Accessing Data Validator

```python
validator = ice.validator
```

## Checking for Nulls

Check if specific columns contain null values.

```python
import polars as pl

df = pl.DataFrame(...)

if not ice.validator.check_nulls(df, ["id", "created_at"]):
    print("Data contains nulls in required columns!")
```

## Validating Constraints

Validate data against SQL-like constraints or custom functions.

```python
import polars as pl

df = pl.DataFrame(...)

results = ice.validator.validate(df, [
    pl.col("age") > 0,
    pl.col("status").is_in(["active", "inactive"])
])

if not results["passed"]:
    print("Validation failed:", results["details"])
```
