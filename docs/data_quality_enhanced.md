# Enhanced Data Quality

IceFrame provides a robust suite of data validation methods to ensure your data meets quality standards before or after ingestion.

## Usage

Access the validator via `ice.quality`. You can pass DataFrames, QueryBuilders, or SQL strings directly to expectation methods.

```python
# 1. Using Query Builder
ice.quality.expect_column_values_to_be_unique(
    ice.query("my_table").filter("id > 100"), 
    "id"
)

# 2. Using SQL (DataFusion) (requires DataFusion optional dependency)
ice.quality.expect_column_values_to_match_regex(
    "SELECT email FROM users WHERE active = true",
    "email",
    r"^.+@.+\..+$"
)

# 3. Using Polars DataFrame directly
df = pl.DataFrame(...)
ice.quality.expect_column_values_to_not_be_null(df, "required_col")
```

## Available Expectations

- `expect_column_values_to_be_unique(df, column)`
- `expect_column_values_to_be_between(df, column, min_value, max_value)`
- `expect_column_values_to_match_regex(df, column, regex)`
- `expect_column_values_to_be_in_set(df, column, value_set)`
- `expect_column_values_to_not_be_null(df, column)`
- `expect_table_row_count_to_be_between(df, min_value, max_value)`
