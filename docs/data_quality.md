# Data Quality Hooks

IceFrame allows you to enforce data quality rules at write time. By attaching validators to `append_to_table`, you can reject invalid data before it corrupts your table.

## Null semantics (changed in 0.13.0)

**A null value fails a constraint by default.**

Before 0.13.0, violations were computed as `df.filter(~expr)`. Under Polars'
three-valued logic `~expr` evaluates to *null* when the input is null, so the
row was filtered out of the violations frame and the constraint **passed**:

```python
# 0.12.0 and earlier: {"passed": True}   <- a null age satisfied "age > 0"
# 0.13.0 and later:   {"passed": False}
DataValidator().validate(pl.DataFrame({"age": [5, None]}), ["age > 0"])
```

That is exactly backwards for a quality gate, and it silently weakened
`append_to_table(validators=[...])`, which blocks writes.

If you genuinely want SQL `CHECK` semantics, where nulls are ignored, opt in:

```python
DataValidator().validate(df, ["age > 0"], null_policy="pass")
DataValidator(null_policy="pass").validate(df, ["age > 0"])
```

Validators that appeared to pass before this release may now correctly fail.
That is the bug being fixed, not a regression.

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
except ValidationError as e:     # ValidationError subclasses ValueError
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
