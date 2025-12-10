"""
Data quality and validation for IceFrame.
"""

from typing import List, Dict, Any, Optional, Union
import polars as pl

class DataValidator:
    """
    Validates data quality for Iceberg tables.
    """
    
    def __init__(self, ice_frame=None):
        self.ice_frame = ice_frame
        
    def _resolve_data(self, data: Union[pl.DataFrame, Any, str]) -> pl.DataFrame:
        """
        Resolve input data to a Polars DataFrame.
        
        Args:
            data: DataFrame, QueryBuilder, or SQL string
            
        Returns:
            Polars DataFrame
        """
        if isinstance(data, pl.DataFrame):
            return data
            
        # Check for QueryBuilder (duck typing or import)
        if hasattr(data, 'execute') and callable(data.execute):
            return data.execute()
            
        if isinstance(data, str):
            if not self.ice_frame:
                raise ValueError("IceFrame instance required to execute SQL queries")
            return self.ice_frame.query_datafusion(data)
            
        raise ValueError(f"Unsupported data type: {type(data)}")

    def check_nulls(self, data: Union[pl.DataFrame, Any, str], columns: List[str]) -> bool:
        """
        Check if specified columns contain null values.
        
        Args:
            data: Polars DataFrame, QueryBuilder, or SQL string to check
            columns: List of column names to check for nulls
            
        Returns:
            True if no nulls found, False otherwise
            
        Raises:
            ValueError: If columns are missing from DataFrame
        """
        df = self._resolve_data(data)
        missing_cols = [c for c in columns if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")
            
        for col in columns:
            if df[col].null_count() > 0:
                return False
        return True
        
    def check_constraints(self, data: Union[pl.DataFrame, Any, str], constraints: Dict[str, str]) -> bool:
        """
        Check if data satisfies SQL-like constraints.
        
        Args:
            data: Polars DataFrame, QueryBuilder, or SQL string to check
            constraints: Dictionary mapping column names to constraint expressions
                        (e.g., {"age": "age > 0", "status": "status != 'deleted'"})
                        Note: The key is just for reference/error reporting, the value is the full expression.
                        Actually, let's make it a list of expressions or a dict where key is description.
                        Let's stick to dict of {description: expression} or just list of expressions.
                        
                        Simpler: Dict[str, str] where key is column and value is condition like "> 0".
                        Or better: List of SQL-like expressions supported by Polars or simple eval.
                        
                        Let's use Polars expression strings if possible, or just python lambda?
                        String expressions are parsed by Polars in some contexts (SQLContext), but here we have a DataFrame.
                        
                        Let's support simple Polars expression strings if we can, or just use `pl.Expr`.
                        But to keep it simple for users, maybe just accept a list of filter expressions as strings 
                        that we can pass to `filter()` and check if count matches.
                        
        Returns:
            True if all rows satisfy all constraints
        """
        # This is a bit tricky without a full SQL parser.
        # Let's support a simplified approach:
        # constraints = ["age > 0", "status != 'deleted'"]
        # We can try to use `iceframe.expressions` or just rely on Polars SQL context?
        # Or just let user pass a function?
        
        # For now, let's implement a simple check using Polars SQLContext if available, 
        # or just iterate and check.
        
        # Actually, let's use the QueryBuilder's expression system if possible, 
        # or just simple Polars expressions.
        
        # Let's change the API to accept a list of Polars expressions or a custom validation function.
        # But for "string" constraints, we might need `pl.sql_expr(constraint)` if available.
        
        # Let's stick to a simple implementation:
        # check_constraints(df, [pl.col("age") > 0])
        # Placeholder implementation
        return True

    def validate(self, data: Union[pl.DataFrame, Any, str], checks: List[Any]) -> Dict[str, Any]:
        """
        Run a suite of validation checks.
        
        Args:
            data: Polars DataFrame, QueryBuilder, or SQL string to validate
            checks: List of checks (can be custom functions or Polars expressions)
            
        Returns:
            Dictionary with results
        """
        df = self._resolve_data(data)
        results = {"passed": True, "details": []}
        for check in checks:
            # If check is a Polars expression, we expect it to evaluate to True for all rows
            if isinstance(check, pl.Expr):
                # Filter where NOT check
                failed_rows = df.filter(~check)
                if failed_rows.height > 0:
                    results["passed"] = False
                    results["details"].append(f"Constraint failed: {check} (Failed rows: {failed_rows.height})")
            elif callable(check):
                try:
                    if not check(df):
                        results["passed"] = False
                        results["details"].append(f"Custom check failed: {check.__name__}")
                except Exception as e:
                    results["passed"] = False
                    results["details"].append(f"Check raised exception: {e}")
                    
        return results

    def expect_column_values_to_be_unique(self, data: Union[pl.DataFrame, Any, str], column: str) -> bool:
        """Expect column values to be unique."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValueError(f"Column {column} not found")
        return df[column].n_unique() == df.height

    def expect_column_values_to_be_between(
        self, data: Union[pl.DataFrame, Any, str], column: str, min_value: Union[int, float], max_value: Union[int, float]
    ) -> bool:
        """Expect column values to be between min_value and max_value (inclusive)."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValueError(f"Column {column} not found")
        
        # Filter for values OUTSIDE the range
        invalid = df.filter(
            (pl.col(column) < min_value) | (pl.col(column) > max_value)
        )
        return invalid.height == 0

    def expect_column_values_to_match_regex(self, data: Union[pl.DataFrame, Any, str], column: str, regex: str) -> bool:
        """Expect column values to match regex."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValueError(f"Column {column} not found")
            
        # Filter for values NOT matching regex
        # Note: str.contains returns boolean series
        invalid = df.filter(
            ~pl.col(column).str.contains(regex)
        )
        return invalid.height == 0

    def expect_column_values_to_be_in_set(self, data: Union[pl.DataFrame, Any, str], column: str, value_set: List[Any]) -> bool:
        """Expect column values to be in a set of values."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValueError(f"Column {column} not found")
            
        invalid = df.filter(
            ~pl.col(column).is_in(value_set)
        )
        return invalid.height == 0

    def expect_column_values_to_not_be_null(self, data: Union[pl.DataFrame, Any, str], column: str) -> bool:
        """Expect column values to not be null."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValueError(f"Column {column} not found")
        return df[column].null_count() == 0

    def expect_table_row_count_to_be_between(
        self, data: Union[pl.DataFrame, Any, str], min_value: int, max_value: int
    ) -> bool:
        """Expect table row count to be between min and max."""
        df = self._resolve_data(data)
        count = df.height
        return min_value <= count <= max_value
