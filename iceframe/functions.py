"""
Standard SQL functions, window functions, and case statements for IceFrame Query API.
"""

from typing import Any, List, Optional, Set, Tuple, Union

import polars as pl
from pyiceberg.expressions import AlwaysTrue

from iceframe.expressions import Expression, LiteralValue


class Function(Expression):
    """Base class for functions.

    No function (aggregate, window, or CASE) can be evaluated by an Iceberg
    scan, so all of them report themselves as *not* pushable. They used to
    return ``None`` from ``to_iceberg()``, which would have blown up with
    ``And(None, ...)`` had one ever reached a filter list.
    """

    def pushdown(self) -> Tuple[Any, bool]:
        return AlwaysTrue(), False

    def to_iceberg(self):
        return AlwaysTrue()


class AggregateFunction(Function):
    """Base class for aggregate functions"""

    #: Set by subclasses; the single operand (may be ``None`` for ``count()``).
    expr: Optional[Expression] = None

    def referenced_columns(self) -> Optional[Set[str]]:
        if self.expr is None:
            return set()
        return self.expr.referenced_columns()


class Count(AggregateFunction):
    def __init__(self, expr: Optional[Expression] = None):
        self.expr = expr

    def to_polars(self):
        if self.expr:
            return self.expr.to_polars().count()
        # pl.count() has been deprecated since Polars 0.20.5 in favour of pl.len().
        return pl.len()


class Sum(AggregateFunction):
    def __init__(self, expr: Expression):
        self.expr = expr

    def to_polars(self):
        return self.expr.to_polars().sum()


class Avg(AggregateFunction):
    def __init__(self, expr: Expression):
        self.expr = expr

    def to_polars(self):
        return self.expr.to_polars().mean()


class Min(AggregateFunction):
    def __init__(self, expr: Expression):
        self.expr = expr

    def to_polars(self):
        return self.expr.to_polars().min()


class Max(AggregateFunction):
    def __init__(self, expr: Expression):
        self.expr = expr

    def to_polars(self):
        return self.expr.to_polars().max()


class WindowFunction(Function):
    """
    Base class for window functions.

    ``ORDER BY`` is honoured across *all* order-by expressions, not just the
    first one: the ordering key is a Polars struct of every order expression,
    which gives lexicographic comparison with correct tie semantics.

    ``descending`` may be a single bool (applies to every order key). Mixed
    per-column directions are not supported and raise ``ValueError`` rather
    than silently sorting the wrong way.
    """

    def __init__(self):
        self._partition_by: List[Expression] = []
        self._order_by: List[Expression] = []
        self._descending: bool = False

    def over(
        self,
        partition_by: Optional[Union[Expression, List[Expression]]] = None,
        order_by: Optional[Union[Expression, List[Expression]]] = None,
        descending: Union[bool, List[bool]] = False,
    ):
        if partition_by:
            self._partition_by = partition_by if isinstance(partition_by, list) else [partition_by]
        if order_by:
            self._order_by = order_by if isinstance(order_by, list) else [order_by]
        if isinstance(descending, (list, tuple)):
            if len(set(descending)) > 1:
                raise ValueError(
                    "Mixed per-column sort directions are not supported by IceFrame "
                    "window functions; pass a single bool for `descending`."
                )
            self._descending = bool(descending[0]) if descending else False
        else:
            self._descending = bool(descending)
        return self

    def referenced_columns(self) -> Optional[Set[str]]:
        cols: Set[str] = set()
        for e in list(self._partition_by) + list(self._order_by) + list(self._operands()):
            sub = e.referenced_columns()
            if sub is None:
                return None
            cols |= sub
        return cols

    def _operands(self) -> List[Expression]:
        """Extra expressions this function reads beyond partition/order keys."""
        return []

    def _order_key(self) -> pl.Expr:
        """A single Polars expression that orders rows lexicographically."""
        return pl.struct([e.to_polars() for e in self._order_by])

    def _apply_partition(self, expr: pl.Expr) -> pl.Expr:
        if self._partition_by:
            return expr.over([e.to_polars() for e in self._partition_by])
        return expr

    def _require_order_by(self, name: str) -> None:
        if not self._order_by:
            raise ValueError(f"{name} requires an ORDER BY clause; use .over(order_by=[...])")

    def to_polars(self):
        raise NotImplementedError


class RowNumber(WindowFunction):
    """SQL ``ROW_NUMBER()``. Ties are broken by original row order."""

    def to_polars(self):
        if not self._order_by:
            # Unordered ROW_NUMBER: number rows in physical order within each
            # partition. Deterministic given the input frame.
            expr = pl.int_range(1, pl.len() + 1, dtype=pl.UInt32)
            return self._apply_partition(expr)

        expr = self._order_key().rank("ordinal", descending=self._descending)
        return self._apply_partition(expr)


class Rank(WindowFunction):
    """SQL ``RANK()`` — ties share the lower rank and leave gaps (1, 1, 3)."""

    def to_polars(self):
        self._require_order_by("Rank")
        expr = self._order_key().rank(method="min", descending=self._descending)
        return self._apply_partition(expr)


class DenseRank(WindowFunction):
    """SQL ``DENSE_RANK()`` — ties share a rank, no gaps (1, 1, 2)."""

    def to_polars(self):
        self._require_order_by("DenseRank")
        expr = self._order_key().rank(method="dense", descending=self._descending)
        return self._apply_partition(expr)


class _Offset(WindowFunction):
    """Shared implementation for LEAD/LAG."""

    #: +1 for LEAD (look forward), -1 for LAG (look backward).
    _direction = 1

    def __init__(self, expr: Expression, offset: int = 1, default: Any = None):
        super().__init__()
        if offset < 0:
            raise ValueError("offset must be >= 0")
        self.expr = expr
        self.offset = offset
        self.default = default

    def _operands(self) -> List[Expression]:
        return [self.expr]

    def to_polars(self):
        value = self.expr.to_polars()
        shift_by = -self.offset * self._direction

        if not self._order_by:
            shifted = value.shift(shift_by, fill_value=self.default)
            return self._apply_partition(shifted)

        order_exprs = [e.to_polars() for e in self._order_by]
        # Sort the values into window order, shift, then scatter each result
        # back to the row it belongs to via that row's ordinal rank.
        ordered = value.sort_by(order_exprs, descending=self._descending).shift(
            shift_by, fill_value=self.default
        )
        position = self._order_key().rank("ordinal", descending=self._descending) - 1
        return self._apply_partition(ordered.gather(position))


class Lead(_Offset):
    """SQL ``LEAD(expr, offset, default)``."""

    _direction = 1


class Lag(_Offset):
    """SQL ``LAG(expr, offset, default)``."""

    _direction = -1


class Case(Expression):
    """Case / When / Then / Otherwise expression"""

    def __init__(self):
        self._conditions = []
        self._values = []
        self._otherwise = None

    def when(self, condition: Expression, value: Any):
        self._conditions.append(condition)
        self._values.append(value if isinstance(value, Expression) else LiteralValue(value))
        return self

    def otherwise(self, value: Any):
        self._otherwise = value if isinstance(value, Expression) else LiteralValue(value)
        return self

    def referenced_columns(self) -> Optional[Set[str]]:
        cols: Set[str] = set()
        for e in list(self._conditions) + list(self._values) + (
            [self._otherwise] if self._otherwise is not None else []
        ):
            sub = e.referenced_columns()
            if sub is None:
                return None
            cols |= sub
        return cols

    def to_polars(self):
        if not self._conditions:
            raise ValueError("Case expression must have at least one WHEN clause")

        # Start the chain
        expr = pl.when(self._conditions[0].to_polars()).then(self._values[0].to_polars())

        # Add remaining conditions
        for cond, val in zip(self._conditions[1:], self._values[1:]):
            expr = expr.when(cond.to_polars()).then(val.to_polars())

        # Add otherwise
        if self._otherwise:
            expr = expr.otherwise(self._otherwise.to_polars())
        else:
            expr = expr.otherwise(None)

        return expr


# Factory functions

def count(expr: Optional[Expression] = None) -> Count:
    return Count(expr)

def sum(expr: Expression) -> Sum:
    return Sum(expr)

def avg(expr: Expression) -> Avg:
    return Avg(expr)

def min(expr: Expression) -> Min:
    return Min(expr)

def max(expr: Expression) -> Max:
    return Max(expr)

def row_number() -> RowNumber:
    return RowNumber()

def rank() -> Rank:
    return Rank()

def dense_rank() -> DenseRank:
    return DenseRank()

def lead(expr: Expression, offset: int = 1, default: Any = None) -> Lead:
    return Lead(expr, offset=offset, default=default)

def lag(expr: Expression, offset: int = 1, default: Any = None) -> Lag:
    return Lag(expr, offset=offset, default=default)

def when(condition: Expression, value: Any) -> Case:
    return Case().when(condition, value)
