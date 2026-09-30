"""
Query optimization for IceFrame.
"""

from typing import Any

from iceframe.expressions import Column, Expression


class QueryOptimizer:
    """
    Optimize query execution plans.
    """

    def __init__(self):
        self.optimizations_applied: list[str] = []

    def optimize_column_projection(
        self,
        select_exprs: list[Expression],
        filter_exprs: list[Expression],
        group_by_exprs: list[Expression],
    ) -> list[str] | None:
        """
        Determine minimal set of columns needed.

        Returns:
            List of column names to read
        """
        columns = set()

        # Extract columns from all expressions
        for expr_list in [select_exprs, filter_exprs, group_by_exprs]:
            for expr in expr_list:
                if isinstance(expr, Column):
                    columns.add(expr.name)

        self.optimizations_applied.append("column_projection")
        return list(columns) if columns else None

    def analyze_query(
        self, table_name: str, select_exprs: list, filter_exprs: list, group_by_exprs: list
    ) -> dict[str, Any]:
        """
        Analyze query and provide optimization suggestions.

        Returns:
            Dictionary with analysis results
        """
        suggestions: list[str] = []
        analysis: dict[str, Any] = {
            "table": table_name,
            "has_filters": len(filter_exprs) > 0,
            "has_aggregations": len(group_by_exprs) > 0,
            "suggestions": suggestions,
        }

        if not filter_exprs:
            suggestions.append("Consider adding filters to reduce data scanned")

        if select_exprs and not group_by_exprs:
            # Check if selecting all columns
            suggestions.append("Use column projection to select only needed columns")

        return analysis
