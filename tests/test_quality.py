"""
Unit tests for Data Quality
"""

import pytest
import polars as pl
from iceframe.quality import DataValidator

def test_check_nulls():
    """Test checking for nulls"""
    validator = DataValidator()
    
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", None]
    })
    
    assert validator.check_nulls(df, ["id"]) is True
    assert validator.check_nulls(df, ["name"]) is False
    assert validator.check_nulls(df, ["id", "name"]) is False

def test_validate_constraints():
    """Test validating constraints"""
    validator = DataValidator()
    
    df = pl.DataFrame({
        "age": [20, 30, -5],
        "status": ["active", "active", "deleted"]
    })
    
    # Check age > 0
    results = validator.validate(df, [pl.col("age") > 0])
    assert results["passed"] is False
    assert "Constraint failed" in results["details"][0]
    
    # Check status != 'deleted' (fails for last row)
    results = validator.validate(df, [pl.col("status") != "deleted"])
    assert results["passed"] is False
    
    # Check valid data
    df_valid = pl.DataFrame({
        "age": [20, 30],
        "status": ["active", "active"]
    })
    results = validator.validate(df_valid, [
        pl.col("age") > 0,
        pl.col("status") == "active"
    ])
    assert results["passed"] is True
    assert len(results["details"]) == 0

def test_custom_check():
    """Test custom validation function"""
    validator = DataValidator()
    
    df = pl.DataFrame({"val": [1, 2, 3]})
    
    def check_sum(d):
        return d["val"].sum() == 6
        
    results = validator.validate(df, [check_sum])
    assert results["passed"] is True
    
    def check_fail(d):
        return False
        
    results = validator.validate(df, [check_fail])
    assert results["passed"] is False
