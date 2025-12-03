"""
Query optimization for IceFrame.
"""

from typing import List, Dict, Any
from iceframe.expressions import Expression, Column

class QueryOptimizer:
    """
    Optimize query execution plans.
    """
    
    def __init__(self):
        self.optimizations_applied = []
        
    def optimize_column_projection(
        self,
        select_exprs: List[Expression],
        filter_exprs: List[Expression],
        group_by_exprs: List[Expression]
    ) -> List[str]:
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
        self,
        table_name: str,
        select_exprs: List,
        filter_exprs: List,
        group_by_exprs: List
    ) -> Dict[str, Any]:
        """
        Analyze query and provide optimization suggestions.
        
        Returns:
            Dictionary with analysis results
        """
        analysis = {
            "table": table_name,
            "has_filters": len(filter_exprs) > 0,
            "has_aggregations": len(group_by_exprs) > 0,
            "suggestions": []
        }
        
        if not filter_exprs:
            analysis["suggestions"].append("Consider adding filters to reduce data scanned")
            
        if select_exprs and not group_by_exprs:
            # Check if selecting all columns
            analysis["suggestions"].append("Use column projection to select only needed columns")
            
        return analysis
