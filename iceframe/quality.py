"""
Data quality and validation for IceFrame.

**Null semantics.** Under Polars' three-valued logic ``~expr`` evaluates to
*null* (not True) when the input is null, so a row with a null value used to
be filtered out of the "violations" frame and the constraint passed. That is
exactly backwards for a quality gate — and it silently weakened
``append_to_table(validators=[...])``, which blocks writes.

Every constraint here therefore treats a null as a **failure** by default:
violations are ``expr.is_null() | ~expr``. Pass ``null_policy="pass"`` (per
validator or per call) to restore the lenient behaviour where nulls are
ignored.
"""

import logging
from typing import Any, Dict, List, Optional, Union

import polars as pl

from iceframe.exceptions import ValidationError

logger = logging.getLogger(__name__)

#: Accepted values for ``null_policy``.
NULL_POLICIES = ("fail", "pass")


def _violations(expr: pl.Expr, null_policy: str = "fail") -> pl.Expr:
    """
    Build the predicate that selects rows *violating* ``expr``.

    With ``null_policy="fail"`` (the default) a null result counts as a
    violation. With ``"pass"`` nulls are ignored, matching SQL ``CHECK``
    semantics.
    """
    if null_policy not in NULL_POLICIES:
        raise ValidationError(
            f"Invalid null_policy {null_policy!r}; expected one of {NULL_POLICIES}"
        )
    if null_policy == "pass":
        return ~expr.fill_null(True)
    return ~expr.fill_null(False)


class DataValidator:
    """
    Validates data quality for Iceberg tables.

    Args:
        ice_frame: Optional IceFrame instance, needed to resolve SQL strings.
        null_policy: ``"fail"`` (default) treats null values as constraint
            violations; ``"pass"`` ignores them.
    """

    def __init__(self, ice_frame=None, null_policy: str = "fail"):
        if null_policy not in NULL_POLICIES:
            raise ValidationError(
                f"Invalid null_policy {null_policy!r}; expected one of {NULL_POLICIES}"
            )
        self.ice_frame = ice_frame
        self.null_policy = null_policy

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
                raise ValidationError("IceFrame instance required to execute SQL queries")
            return self.ice_frame.query_datafusion(data)

        raise ValidationError(f"Unsupported data type: {type(data)}")

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
            raise ValidationError(f"Columns not found in DataFrame: {missing_cols}")

        for col in columns:
            if df[col].null_count() > 0:
                return False
        return True

    def check_constraints(
        self,
        data: Union[pl.DataFrame, Any, str],
        constraints: Union[Dict[str, str], List[str], List[pl.Expr]],
        null_policy: Optional[str] = None,
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

            null_policy: ``"fail"`` (default) counts null results as
                violations; ``"pass"`` ignores them. Overrides the validator's
                own policy for this call.

        Returns:
            ``True`` only if every constraint holds for every row.
        """
        df = self._resolve_data(data)
        policy = null_policy or self.null_policy

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
            # A constraint holds when every row satisfies it; equivalently,
            # no row matches the violation predicate. Nulls count as violations
            # unless the caller opted into null_policy="pass".
            if df.filter(_violations(expr, policy)).height > 0:
                return False
        return True

    def validate(
        self,
        data: Union[pl.DataFrame, Any, str],
        checks: List[Any],
        null_policy: Optional[str] = None,
    ) -> Dict[str, Any]:
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

        Null handling:
            A null value makes a constraint **fail** by default — a quality
            gate that passes nulls provides false assurance. Pass
            ``null_policy="pass"`` to ignore nulls instead.

        Returns:
            ``{"passed": bool, "details": [str, ...]}``
        """
        df = self._resolve_data(data)
        policy = null_policy or self.null_policy
        results: Dict[str, Any] = {"passed": True, "details": []}

        def _fail(msg: str) -> None:
            results["passed"] = False
            results["details"].append(msg)

        for check in checks:
            if isinstance(check, pl.Expr):
                failed_rows = df.filter(_violations(check, policy))
                if failed_rows.height > 0:
                    _fail(f"Constraint failed: {check} (failed rows: {failed_rows.height})")

            elif isinstance(check, str):
                try:
                    expr = pl.sql_expr(check)
                    failed_rows = df.filter(_violations(expr, policy))
                    if failed_rows.height > 0:
                        _fail(f"Constraint failed: {check!r} (failed rows: {failed_rows.height})")
                except Exception as e:
                    _fail(f"Could not evaluate constraint {check!r}: {e}")

            elif isinstance(check, dict):
                try:
                    if not self._check_dict_constraint(df, check, policy):
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

    def _check_dict_constraint(
        self, df: pl.DataFrame, c: Dict[str, Any], null_policy: Optional[str] = None
    ) -> bool:
        """Evaluate a single dict-shaped constraint. Returns True iff it holds.

        Nulls fail every value constraint unless ``null_policy="pass"``."""
        policy = null_policy or self.null_policy
        ctype = c.get("type")
        col = c.get("column")
        if ctype is None:
            raise ValidationError("Dict constraint requires a 'type' key")
        if col is None and ctype != "row_count":
            raise ValidationError(
                f"Dict constraint of type {ctype!r} requires a 'column' key"
            )
        if col is not None and col not in df.columns:
            raise ValidationError(f"Column {col!r} not found in DataFrame")

        if ctype == "not_null":
            return df[col].null_count() == 0
        if ctype == "unique":
            return df[col].n_unique() == df.height
        if ctype == "between":
            lo, hi = c["min"], c["max"]
            holds = (pl.col(col) >= lo) & (pl.col(col) <= hi)
            return df.filter(_violations(holds, policy)).height == 0
        if ctype == "in_set":
            holds = pl.col(col).is_in(c["values"])
            return df.filter(_violations(holds, policy)).height == 0
        if ctype == "regex":
            holds = pl.col(col).str.contains(c["pattern"])
            return df.filter(_violations(holds, policy)).height == 0
        if ctype == "row_count":
            return c.get("min", 0) <= df.height <= c.get("max", float("inf"))
        raise ValidationError(f"Unknown constraint type: {ctype!r}")

    def expect_column_values_to_be_unique(self, data: Union[pl.DataFrame, Any, str], column: str) -> bool:
        """Expect column values to be unique."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValidationError(f"Column {column} not found")
        return df[column].n_unique() == df.height

    def expect_column_values_to_be_between(
        self, data: Union[pl.DataFrame, Any, str], column: str, min_value: Union[int, float], max_value: Union[int, float]
    ) -> bool:
        """Expect column values to be between min_value and max_value (inclusive)."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValidationError(f"Column {column} not found")

        holds = (pl.col(column) >= min_value) & (pl.col(column) <= max_value)
        return df.filter(_violations(holds, self.null_policy)).height == 0

    def expect_column_values_to_match_regex(self, data: Union[pl.DataFrame, Any, str], column: str, regex: str) -> bool:
        """Expect column values to match regex."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValidationError(f"Column {column} not found")

        holds = pl.col(column).str.contains(regex)
        return df.filter(_violations(holds, self.null_policy)).height == 0

    def expect_column_values_to_be_in_set(self, data: Union[pl.DataFrame, Any, str], column: str, value_set: List[Any]) -> bool:
        """Expect column values to be in a set of values."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValidationError(f"Column {column} not found")

        holds = pl.col(column).is_in(value_set)
        return df.filter(_violations(holds, self.null_policy)).height == 0

    def expect_column_values_to_not_be_null(self, data: Union[pl.DataFrame, Any, str], column: str) -> bool:
        """Expect column values to not be null."""
        df = self._resolve_data(data)
        if column not in df.columns:
            raise ValidationError(f"Column {column} not found")
        return df[column].null_count() == 0

    def expect_table_row_count_to_be_between(
        self, data: Union[pl.DataFrame, Any, str], min_value: int, max_value: int
    ) -> bool:
        """Expect table row count to be between min and max."""
        df = self._resolve_data(data)
        count = df.height
        return min_value <= count <= max_value
