"""
Expression builder for IceFrame Query API.

This module provides a unified expression system that can be translated to:
1. PyIceberg expressions for predicate pushdown
2. Polars expressions for local processing
"""

from typing import Any, Optional, List, Union
import polars as pl
from pyiceberg.expressions import (
    EqualTo,
    NotEqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    IsNull,
    NotNull,
    In,
    NotIn,
    And,
    Or,
    Not,
    AlwaysTrue,
    AlwaysFalse,
    Reference,
    Literal,
)


class Expression:
    """Base class for all expressions"""
    
    def to_iceberg(self) -> Any:
        """Convert to PyIceberg expression"""
        raise NotImplementedError
    
    def to_polars(self) -> pl.Expr:
        """Convert to Polars expression"""
        raise NotImplementedError
    
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
    
    def to_iceberg(self):
        return Reference(self.name)
    
    def to_polars(self):
        return pl.col(self.name)


class LiteralValue(Expression):
    """Represents a literal value"""
    
    def __init__(self, value: Any):
        self.value = value
    
    def to_iceberg(self):
        return Literal(self.value)
    
    def to_polars(self):
        return pl.lit(self.value)


class BinaryExpression(Expression):
    """Represents a binary operation (e.g., a == b)"""
    
    def __init__(self, left: Expression, right: Any, op: str):
        self.left = left
        self.right = right if isinstance(right, Expression) else LiteralValue(right)
        self.op = op
    
    def to_iceberg(self):
        # PyIceberg expressions typically expect Reference on left and Literal on right
        # for simple predicates. Complex expressions might not be fully supported for pushdown.
        if not isinstance(self.left, Column):
            # If left is not a column, we can't push down easily to PyIceberg
            # Return AlwaysTrue to fetch all data and filter locally
            return AlwaysTrue()
            
        col_name = self.left.name
        val = self.right.value if isinstance(self.right, LiteralValue) else None
        
        if val is None:
            # Right side is not a literal, can't push down
            return AlwaysTrue()
            
        if self.op == "eq":
            return EqualTo(col_name, val)
        elif self.op == "ne":
            return NotEqualTo(col_name, val)
        elif self.op == "gt":
            return GreaterThan(col_name, val)
        elif self.op == "ge":
            return GreaterThanOrEqual(col_name, val)
        elif self.op == "lt":
            return LessThan(col_name, val)
        elif self.op == "le":
            return LessThanOrEqual(col_name, val)
        
        return AlwaysTrue()
    
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


class BooleanExpression(Expression):
    """Represents boolean operations (AND, OR)"""
    
    def __init__(self, left: Expression, right: Expression, op: str):
        self.left = left
        self.right = right
        self.op = op
    
    def to_iceberg(self):
        left_ice = self.left.to_iceberg()
        right_ice = self.right.to_iceberg()
        
        if self.op == "and":
            return And(left_ice, right_ice)
        elif self.op == "or":
            return Or(left_ice, right_ice)
            
        return AlwaysTrue()
    
    def to_polars(self):
        left_pl = self.left.to_polars()
        right_pl = self.right.to_polars()
        
        if self.op == "and":
            return left_pl & right_pl
        elif self.op == "or":
            return left_pl | right_pl
            
        raise ValueError(f"Unknown boolean operator: {self.op}")


class NotExpression(Expression):
    """Represents NOT operation"""
    
    def __init__(self, expr: Expression):
        self.expr = expr
    
    def to_iceberg(self):
        return Not(self.expr.to_iceberg())
    
    def to_polars(self):
        return ~self.expr.to_polars()


class InExpression(Expression):
    """Represents IN operation"""
    
    def __init__(self, expr: Expression, values: List[Any]):
        self.expr = expr
        self.values = values
    
    def to_iceberg(self):
        if isinstance(self.expr, Column):
            return In(self.expr.name, self.values)
        return AlwaysTrue()
    
    def to_polars(self):
        return self.expr.to_polars().is_in(self.values)


class IsNullExpression(Expression):
    """Represents IS NULL operation"""
    
    def __init__(self, expr: Expression):
        self.expr = expr
    
    def to_iceberg(self):
        if isinstance(self.expr, Column):
            return IsNull(self.expr.name)
        return AlwaysTrue()
    
    def to_polars(self):
        return self.expr.to_polars().is_null()


class IsNotNullExpression(Expression):
    """Represents IS NOT NULL operation"""
    
    def __init__(self, expr: Expression):
        self.expr = expr
    
    def to_iceberg(self):
        if isinstance(self.expr, Column):
            return NotNull(self.expr.name)
        return AlwaysTrue()
    
    def to_polars(self):
        return self.expr.to_polars().is_not_null()


class AliasExpression(Expression):
    """Represents column aliasing"""
    
    def __init__(self, expr: Expression, name: str):
        self.expr = expr
        self.name = name
    
    def to_iceberg(self):
        # Aliasing doesn't affect predicate pushdown
        return self.expr.to_iceberg()
    
    def to_polars(self):
        return self.expr.to_polars().alias(self.name)


def col(name: str) -> Column:
    """Create a column reference"""
    return Column(name)


def lit(value: Any) -> LiteralValue:
    """Create a literal value"""
    return LiteralValue(value)
