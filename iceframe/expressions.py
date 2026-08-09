"""
Expression builder for IceFrame Query API.

This module provides a unified expression system that can be translated to:
1. PyIceberg expressions for predicate pushdown
2. Polars expressions for local processing
"""

from typing import Any, List, Optional, Set, Tuple

import polars as pl
from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Literal,
    Not,
    NotEqualTo,
    NotNull,
    Or,
    Reference,
)


class Expression:
    """Base class for all expressions"""

    def pushdown(self) -> Tuple[Any, bool]:
        """
        Return ``(iceberg_expression, fully_pushed)``.

        ``iceberg_expression`` is always a *safe superset* of this predicate:
        scanning with it never drops a row the predicate would have kept. When
        ``fully_pushed`` is ``False`` the caller **must** also evaluate this
        expression locally (via :meth:`to_polars`), because the pushed
        expression is weaker than the real predicate.

        This exists because ``AlwaysTrue`` cannot be used as a "not pushable"
        sentinel: PyIceberg simplifies ``And(AlwaysTrue(), X)`` to ``X``, so an
        unpushable operand ANDed with a pushable one used to vanish entirely
        and the query silently returned wrong rows.
        """
        return self.to_iceberg(), True

    def to_iceberg(self) -> Any:
        """Convert to PyIceberg expression (safe superset; see :meth:`pushdown`)."""
        raise NotImplementedError

    def to_polars(self) -> pl.Expr:
        """Convert to Polars expression"""
        raise NotImplementedError

    def referenced_columns(self) -> Optional[Set[str]]:
        """
        Names of the table columns this expression reads, or ``None`` when that
        can't be determined statically.

        ``None`` is the conservative answer and disables projection pushdown —
        never return an empty set to mean "I don't know".
        """
        return None

    def __eq__(self, other):
        return BinaryExpression(self, other, "eq")

    def __ne__(self, other):
        return BinaryExpression(self, other, "ne")

    def __gt__(self, other):
        return BinaryExpression(self, other, "gt")

    def __ge__(self, other):
        return BinaryExpression(self, other, "ge")

    def __lt__(self, other):
        return BinaryExpression(self, other, "lt")

    def __le__(self, other):
        return BinaryExpression(self, other, "le")

    def __and__(self, other):
        return BooleanExpression(self, other, "and")

    def __or__(self, other):
        return BooleanExpression(self, other, "or")

    def __invert__(self):
        return NotExpression(self)

    def is_in(self, values: List[Any]):
        return InExpression(self, values)

    def is_null(self):
        return IsNullExpression(self)

    def is_not_null(self):
        return IsNotNullExpression(self)

    def alias(self, name: str):
        return AliasExpression(self, name)


class Column(Expression):
    """Represents a column reference"""

    def __init__(self, name: str):
        self.name = name

    def pushdown(self) -> Tuple[Any, bool]:
        # A bare column reference is not a valid Iceberg row filter (it's a
        # term, not a predicate). If someone filters on a boolean column
        # directly, evaluate it locally rather than handing PyIceberg a
        # Reference it will reject.
        return AlwaysTrue(), False

    def to_iceberg(self):
        return Reference(self.name)


    def to_polars(self):
        return pl.col(self.name)

    def referenced_columns(self):
        return {self.name}


class LiteralValue(Expression):
    """Represents a literal value"""

    def __init__(self, value: Any):
        self.value = value

    def pushdown(self) -> Tuple[Any, bool]:
        # A literal is a term, not a predicate — never pushable on its own.
        return AlwaysTrue(), False

    def to_iceberg(self):
        return Literal(self.value)


    def to_polars(self):
        return pl.lit(self.value)

    def referenced_columns(self):
        return set()


class BinaryExpression(Expression):
    """Represents a binary operation (e.g., a == b)"""

    def __init__(self, left: Expression, right: Any, op: str):
        self.left = left
        self.right = right if isinstance(right, Expression) else LiteralValue(right)
        self.op = op

    def pushdown(self) -> Tuple[Any, bool]:
        # PyIceberg expressions expect a Reference on the left and a Literal on
        # the right for simple predicates. Anything else (column-to-column
        # comparison, expression on the left) can't be pushed down, so we push
        # AlwaysTrue and tell the caller to apply this predicate locally.
        if not isinstance(self.left, Column):
            return AlwaysTrue(), False

        col_name = self.left.name
        val = self.right.value if isinstance(self.right, LiteralValue) else None

        if val is None:
            # Right side is not a literal (or is an untyped null) — not pushable.
            return AlwaysTrue(), False

        op_map = {
            "eq": EqualTo,
            "ne": NotEqualTo,
            "gt": GreaterThan,
            "ge": GreaterThanOrEqual,
            "lt": LessThan,
            "le": LessThanOrEqual,
        }
        builder = op_map.get(self.op)
        if builder is None:
            return AlwaysTrue(), False
        return builder(col_name, val), True

    def to_iceberg(self):
        return self.pushdown()[0]


    def to_polars(self):
        left_expr = self.left.to_polars()
        right_expr = self.right.to_polars()

        if self.op == "eq":
            return left_expr == right_expr
        elif self.op == "ne":
            return left_expr != right_expr
        elif self.op == "gt":
            return left_expr > right_expr
        elif self.op == "ge":
            return left_expr >= right_expr
        elif self.op == "lt":
            return left_expr < right_expr
        elif self.op == "le":
            return left_expr <= right_expr

        raise ValueError(f"Unknown operator: {self.op}")

    def referenced_columns(self):
        left = self.left.referenced_columns()
        right = self.right.referenced_columns()
        if left is None or right is None:
            return None
        return left | right


class BooleanExpression(Expression):
    """Represents boolean operations (AND, OR)"""

    def __init__(self, left: Expression, right: Expression, op: str):
        self.left = left
        self.right = right
        self.op = op

    def pushdown(self) -> Tuple[Any, bool]:
        left_ice, left_ok = self.left.pushdown()
        right_ice, right_ok = self.right.pushdown()
        fully = left_ok and right_ok

        if self.op == "and":
            # And(AlwaysTrue(), X) simplifies to X, which is still a safe
            # superset of "X AND <unpushable>" — but only because we report
            # fully=False so the caller re-applies the whole predicate locally.
            return And(left_ice, right_ice), fully
        elif self.op == "or":
            # Or(AlwaysTrue(), X) simplifies to AlwaysTrue — also a safe
            # superset.
            return Or(left_ice, right_ice), fully

        return AlwaysTrue(), False

    def to_iceberg(self):
        return self.pushdown()[0]


    def to_polars(self):
        left_pl = self.left.to_polars()
        right_pl = self.right.to_polars()

        if self.op == "and":
            return left_pl & right_pl
        elif self.op == "or":
            return left_pl | right_pl

        raise ValueError(f"Unknown boolean operator: {self.op}")

    def referenced_columns(self):
        left = self.left.referenced_columns()
        right = self.right.referenced_columns()
        if left is None or right is None:
            return None
        return left | right


class NotExpression(Expression):
    """Represents NOT operation"""

    def __init__(self, expr: Expression):
        self.expr = expr

    def pushdown(self) -> Tuple[Any, bool]:
        # Negating a partially-pushed predicate is not sound: Not() of a
        # superset is a *subset* of the real answer, which would silently drop
        # rows. So a NOT is only pushable when its operand pushes fully.
        inner, inner_ok = self.expr.pushdown()
        if not inner_ok:
            return AlwaysTrue(), False
        if isinstance(inner, (AlwaysTrue, AlwaysFalse)):
            return AlwaysTrue(), False
        return Not(inner), True

    def to_iceberg(self):
        return self.pushdown()[0]

    def to_polars(self):
        return ~self.expr.to_polars()

    def referenced_columns(self):
        return self.expr.referenced_columns()


class InExpression(Expression):
    """Represents IN operation"""

    def __init__(self, expr: Expression, values: List[Any]):
        self.expr = expr
        self.values = values

    def pushdown(self) -> Tuple[Any, bool]:
        if isinstance(self.expr, Column):
            return In(self.expr.name, self.values), True
        return AlwaysTrue(), False

    def to_iceberg(self):
        return self.pushdown()[0]


    def to_polars(self):
        return self.expr.to_polars().is_in(self.values)

    def referenced_columns(self):
        return self.expr.referenced_columns()


class IsNullExpression(Expression):
    """Represents IS NULL operation"""

    def __init__(self, expr: Expression):
        self.expr = expr

    def pushdown(self) -> Tuple[Any, bool]:
        if isinstance(self.expr, Column):
            return IsNull(self.expr.name), True
        return AlwaysTrue(), False

    def to_iceberg(self):
        return self.pushdown()[0]


    def to_polars(self):
        return self.expr.to_polars().is_null()

    def referenced_columns(self):
        return self.expr.referenced_columns()


class IsNotNullExpression(Expression):
    """Represents IS NOT NULL operation"""

    def __init__(self, expr: Expression):
        self.expr = expr

    def pushdown(self) -> Tuple[Any, bool]:
        if isinstance(self.expr, Column):
            return NotNull(self.expr.name), True
        return AlwaysTrue(), False

    def to_iceberg(self):
        return self.pushdown()[0]


    def to_polars(self):
        return self.expr.to_polars().is_not_null()

    def referenced_columns(self):
        return self.expr.referenced_columns()


class AliasExpression(Expression):
    """Represents column aliasing"""

    def __init__(self, expr: Expression, name: str):
        self.expr = expr
        self.name = name

    def pushdown(self) -> Tuple[Any, bool]:
        # Aliasing doesn't affect predicate pushdown
        return self.expr.pushdown()

    def to_iceberg(self):
        return self.pushdown()[0]


    def to_polars(self):
        return self.expr.to_polars().alias(self.name)

    def referenced_columns(self):
        return self.expr.referenced_columns()


def plan_pushdown(exprs: List["Expression"]) -> Tuple[Any, List["Expression"]]:
    """
    Split a list of IceFrame predicates into a single pushable Iceberg
    ``row_filter`` and the list of predicates that must still be applied
    locally.

    The returned Iceberg expression is always a safe superset: scanning with it
    returns at least every row the full predicate list would keep. Any
    expression that could not be pushed down *in full* is returned in the
    residual list and must be re-applied with Polars.

    Returns:
        ``(iceberg_row_filter, residual_expressions)``
    """
    pushed: List[Any] = []
    residual: List[Expression] = []

    for expr in exprs:
        ice, fully = expr.pushdown()
        if not isinstance(ice, AlwaysTrue):
            pushed.append(ice)
        if not fully:
            # Re-apply the *whole* predicate locally. Applying it twice is
            # harmless (filters are idempotent) and much safer than trying to
            # work out which sub-term was dropped.
            residual.append(expr)

    if not pushed:
        return AlwaysTrue(), residual

    combined = pushed[0]
    for extra in pushed[1:]:
        combined = And(combined, extra)
    return combined, residual


def col(name: str) -> Column:
    """Create a column reference"""
    return Column(name)


def lit(value: Any) -> LiteralValue:
    """Create a literal value"""
    return LiteralValue(value)
