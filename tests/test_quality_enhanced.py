import pytest
import polars as pl
from iceframe.quality import DataValidator

@pytest.fixture
def validator():
    return DataValidator()

@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "david@example.com", "eve@example.com"],
        "category": ["A", "B", "A", "B", "C"],
        "nullable": [1, None, 3, 4, 5]
    })

def test_expect_unique(validator, sample_df):
    assert validator.expect_column_values_to_be_unique(sample_df, "id")
    
    df_dup = pl.DataFrame({"id": [1, 1, 2]})
    assert not validator.expect_column_values_to_be_unique(df_dup, "id")

def test_expect_between(validator, sample_df):
    assert validator.expect_column_values_to_be_between(sample_df, "age", 20, 50)
    assert not validator.expect_column_values_to_be_between(sample_df, "age", 20, 30) # 35, 40, 45 fail

def test_expect_regex(validator, sample_df):
    assert validator.expect_column_values_to_match_regex(sample_df, "email", r"^.+@.+\..+$")
    
    df_bad = pl.DataFrame({"email": ["bad_email"]})
    assert not validator.expect_column_values_to_match_regex(df_bad, "email", r"@")

def test_expect_in_set(validator, sample_df):
    assert validator.expect_column_values_to_be_in_set(sample_df, "category", ["A", "B", "C"])
    assert not validator.expect_column_values_to_be_in_set(sample_df, "category", ["A", "B"]) # C fails

def test_expect_not_null(validator, sample_df):
    assert validator.expect_column_values_to_not_be_null(sample_df, "id")
    assert not validator.expect_column_values_to_not_be_null(sample_df, "nullable")

def test_expect_row_count(validator, sample_df):
    assert validator.expect_table_row_count_to_be_between(sample_df, 1, 10)
    assert not validator.expect_table_row_count_to_be_between(sample_df, 10, 20)
