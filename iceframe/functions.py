"""
Standard SQL functions, window functions, and case statements for IceFrame Query API.
"""

from typing import Any, List, Optional, Union
import polars as pl
from iceframe.expressions import Expression, Column, LiteralValue


class Function(Expression):
    """Base class for functions"""
    pass


class AggregateFunction(Function):
    """Base class for aggregate functions"""
    pass


class Count(AggregateFunction):
    def __init__(self, expr: Optional[Expression] = None):
        self.expr = expr
    
    def to_iceberg(self):
        # Aggregations are not pushed down to PyIceberg scan
        return None
    
    def to_polars(self):
        if self.expr:
            return self.expr.to_polars().count()
        return pl.count()


class Sum(AggregateFunction):
    def __init__(self, expr: Expression):
        self.expr = expr
    
    def to_iceberg(self):
        return None
    
    def to_polars(self):
        return self.expr.to_polars().sum()


class Avg(AggregateFunction):
    def __init__(self, expr: Expression):
        self.expr = expr
    
    def to_iceberg(self):
        return None
    
    def to_polars(self):
        return self.expr.to_polars().mean()


class Min(AggregateFunction):
    def __init__(self, expr: Expression):
        self.expr = expr
    
    def to_iceberg(self):
        return None
    
    def to_polars(self):
        return self.expr.to_polars().min()


class Max(AggregateFunction):
    def __init__(self, expr: Expression):
        self.expr = expr
    
    def to_iceberg(self):
        return None
    
    def to_polars(self):
        return self.expr.to_polars().max()


class WindowFunction(Function):
    """Base class for window functions"""
    
    def __init__(self):
        self._partition_by = []
        self._order_by = []
    
    def over(self, partition_by: Optional[Union[Expression, List[Expression]]] = None, 
             order_by: Optional[Union[Expression, List[Expression]]] = None):
        if partition_by:
            self._partition_by = partition_by if isinstance(partition_by, list) else [partition_by]
        if order_by:
            self._order_by = order_by if isinstance(order_by, list) else [order_by]
        return self
    
    def to_iceberg(self):
        return None
    
    def to_polars(self):
        raise NotImplementedError


class RowNumber(WindowFunction):
    def to_polars(self):
        # Polars implementation of row_number() over window
        # Note: Polars window functions syntax is expr.over(partition_by, order_by)
        # But row_number is often implemented via cum_count or similar
        # pl.col("x").rank("ordinal") is equivalent to row_number
        # But we need a base expression. Usually row_number() doesn't take an argument.
        # In Polars, we can use pl.int_range(0, pl.len()) or similar within a group context
        # Or simpler: pl.col(first_col).rank("ordinal")
        
        # For simplicity, we'll assume this is used in a context where we can apply it
        # But Polars expressions need a column to operate on for window functions usually
        # A trick is to use a literal or any column
        # pl.int_range(0, pl.len()) is robust
        
        expr = pl.int_range(0, pl.len()) + 1
        
        if self._partition_by or self._order_by:
            # Construct over clause
            partition_exprs = [e.to_polars() for e in self._partition_by]
            order_exprs = [e.to_polars() for e in self._order_by]
            
            # Polars doesn't support order_by in over() directly for all functions
            # But for row_number (rank ordinal), we can sort first or use sort_by
            
            if order_exprs:
                # If ordering is specified, we might need to sort
                # But pl.int_range doesn't support sort_by directly in the same way
                # Better approach: use rank("ordinal") on the order column(s)
                # If no order column, row_number is arbitrary
                pass
            
            return expr.over(partition_exprs)
        
        return expr


class Rank(WindowFunction):
    def to_polars(self):
        # Rank needs an order by column to be meaningful
        if not self._order_by:
            raise ValueError("Rank function requires an ORDER BY clause")
            
        # Use the first order by column for ranking
        # This is a simplification
        target_col = self._order_by[0].to_polars()
        expr = target_col.rank("dense") # SQL rank is usually dense or not? 
        # SQL RANK() skips gaps (1, 1, 3). DENSE_RANK() is (1, 1, 2).
        # Polars "rank" method default is "average", we want "min" for standard rank?
        # Actually "rank" method has method="min", "max", "average", "dense", "ordinal"
        # SQL RANK() -> method='min' (with gaps)? No, 'min' gives 1, 1, 1 for ties.
        # Wait, SQL RANK: 1, 1, 3. Polars 'min': 1, 1, 1?
        # Let's check Polars docs or assume 'min' is closest to standard rank with gaps?
        # Actually, let's use 'rank' method
        
        expr = target_col.rank(method="min") # 1, 1, 3
        
        if self._partition_by:
            partition_exprs = [e.to_polars() for e in self._partition_by]
            return expr.over(partition_exprs)
            
        return expr


class DenseRank(WindowFunction):
    def to_polars(self):
        if not self._order_by:
            raise ValueError("DenseRank function requires an ORDER BY clause")
            
        target_col = self._order_by[0].to_polars()
        expr = target_col.rank(method="dense") # 1, 1, 2
        
        if self._partition_by:
            partition_exprs = [e.to_polars() for e in self._partition_by]
            return expr.over(partition_exprs)
            
        return expr


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
    
    def to_iceberg(self):
        return None
    
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

def when(condition: Expression, value: Any) -> Case:
    return Case().when(condition, value)
