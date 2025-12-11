# Data Quality Hooks

IceFrame allows you to enforce data quality rules at write time. By attaching validators to `append_to_table`, you can reject invalid data before it corrupts your table.

## Usage

Pass a list of validation checks to `validators`. These can be **Polars Expressions** (must return True) or **Callable functions** (must return True).

### Using Polars Expressions

Efficient and expressive checks using the Polars expression API.

```python
import polars as pl

# Data must have positive id and non-null name
validators = [
    pl.col("id") > 0,
    pl.col("name").is_not_null()
]

try:
    ice.append_to_table("users", new_users_df, validators=validators)
except ValueError as e:
    print("Validation failed!")
    print(e)
```

### Using Custom Functions

For complex logic not easily expressed in Polars.

```python
def check_email_format(df):
    # Custom python logic checking email column
    return df["email"].str.contains("@").all()

ice.append_to_table("users", df, validators=[check_email_format])
```

## Supported Operations

-   `append_to_table`
-   `create_table_from_*` (Create and Append) - *Coming soon* (Require calling append manually for validation currently)
