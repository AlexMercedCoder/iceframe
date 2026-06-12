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
        
    def check_constraints(
        self,
        data: Union[pl.DataFrame, Any, str],
        constraints: Union[Dict[str, str], List[str], List[pl.Expr]],
    ) -> bool:
        """
        Check whether every row satisfies the given constraints.

        Args:
            data: A Polars DataFrame, QueryBuilder, or SQL string.
            constraints: One of:

                * A dict mapping a description to a Polars SQL boolean
                  expression, e.g. ``{"age_positive": "age > 0",
                  "not_deleted": "status != 'deleted'"}``.
                * A list of Polars SQL boolean expression strings, e.g.
                  ``["age > 0", "status != 'deleted'"]``.
                * A list of ``pl.Expr`` boolean expressions, e.g.
                  ``[pl.col("age") > 0]``.

        Returns:
            ``True`` only if every constraint holds for every row.
        """
        df = self._resolve_data(data)

        if isinstance(constraints, dict):
            iterable = list(constraints.values())
        else:
            iterable = list(constraints)

        for constraint in iterable:
            if isinstance(constraint, pl.Expr):
                expr = constraint
            elif isinstance(constraint, str):
                expr = pl.sql_expr(constraint)
            else:
                raise TypeError(
                    f"Unsupported constraint type: {type(constraint).__name__}. "
                    "Expected str or pl.Expr."
                )
            # A constraint holds when every row satisfies it; equivalently, no
            # row matches the negation.
            if df.filter(~expr).height > 0:
                return False
        return True

    def validate(self, data: Union[pl.DataFrame, Any, str], checks: List[Any]) -> Dict[str, Any]:
        """
        Run a suite of validation checks.

        Args:
            data: Polars DataFrame, QueryBuilder, or SQL string to validate.
            checks: List of checks. Each check may be:

                * a ``pl.Expr`` — the constraint must hold for every row;
                * a string — a Polars SQL boolean expression that must hold;
                * a dict — interpreted as a single constraint of the form
                  ``{"type": "not_null"|"unique"|"between"|"in_set"|"regex", ...}``
                  (see below for required keys);
                * a callable ``f(df) -> bool``.

        Dict-style constraint shapes:
            ``{"type": "not_null", "column": "id"}``
            ``{"type": "unique", "column": "id"}``
            ``{"type": "between", "column": "age", "min": 0, "max": 150}``
            ``{"type": "in_set", "column": "status", "values": ["a", "b"]}``
            ``{"type": "regex", "column": "email", "pattern": ".+@.+"}``

        Returns:
            ``{"passed": bool, "details": [str, ...]}``
        """
        df = self._resolve_data(data)
        results: Dict[str, Any] = {"passed": True, "details": []}

        def _fail(msg: str) -> None:
            results["passed"] = False
            results["details"].append(msg)

        for check in checks:
            if isinstance(check, pl.Expr):
                failed_rows = df.filter(~check)
                if failed_rows.height > 0:
                    _fail(f"Constraint failed: {check} (failed rows: {failed_rows.height})")

            elif isinstance(check, str):
                try:
                    expr = pl.sql_expr(check)
                    failed_rows = df.filter(~expr)
                    if failed_rows.height > 0:
                        _fail(f"Constraint failed: {check!r} (failed rows: {failed_rows.height})")
                except Exception as e:
                    _fail(f"Could not evaluate constraint {check!r}: {e}")

            elif isinstance(check, dict):
                try:
                    if not self._check_dict_constraint(df, check):
                        _fail(f"Dict constraint failed: {check}")
                except Exception as e:
                    _fail(f"Dict constraint raised: {check} -> {e}")

            elif callable(check):
                try:
                    if not check(df):
                        _fail(f"Custom check failed: {getattr(check, '__name__', check)}")
                except Exception as e:
                    _fail(f"Check raised exception: {e}")

            else:
                _fail(f"Unsupported check type: {type(check).__name__}")

        return results

    def _check_dict_constraint(self, df: pl.DataFrame, c: Dict[str, Any]) -> bool:
        """Evaluate a single dict-shaped constraint. Returns True iff it holds."""
        ctype = c.get("type")
        col = c.get("column")
        if ctype is None:
            raise ValueError("Dict constraint requires a 'type' key")
        if col is None and ctype != "row_count":
            raise ValueError(f"Dict constraint of type {ctype!r} requires a 'column' key")
        if col is not None and col not in df.columns:
            raise ValueError(f"Column {col!r} not found in DataFrame")

        if ctype == "not_null":
            return df[col].null_count() == 0
        if ctype == "unique":
            return df[col].n_unique() == df.height
        if ctype == "between":
            lo, hi = c["min"], c["max"]
            invalid = df.filter((pl.col(col) < lo) | (pl.col(col) > hi))
            return invalid.height == 0
        if ctype == "in_set":
            invalid = df.filter(~pl.col(col).is_in(c["values"]))
            return invalid.height == 0
        if ctype == "regex":
            invalid = df.filter(~pl.col(col).str.contains(c["pattern"]))
            return invalid.height == 0
        if ctype == "row_count":
            return c.get("min", 0) <= df.height <= c.get("max", float("inf"))
        raise ValueError(f"Unknown constraint type: {ctype!r}")

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
