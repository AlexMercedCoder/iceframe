"""
Data skipping optimizations for IceFrame.
"""

from typing import List, Any
from iceframe.expressions import Expression, Column, BinaryExpression

class DataSkipper:
    """
    Use table statistics to skip unnecessary data files.
    """
    
    def __init__(self):
        self.files_skipped = 0
        self.files_scanned = 0
        
    def can_skip_file(
        self,
        file_stats: dict,
        filter_expr: Expression
    ) -> bool:
        """
        Determine if a file can be skipped based on statistics.
        
        Args:
            file_stats: File-level statistics (min/max values)
            filter_expr: Filter expression
            
        Returns:
            True if file can be skipped
        """
        # Simple implementation: check min/max bounds
        if isinstance(filter_expr, BinaryExpression):
            if isinstance(filter_expr.left, Column):
                col_name = filter_expr.left.name
                
                if col_name in file_stats:
                    stats = file_stats[col_name]
                    value = filter_expr.right.value if hasattr(filter_expr.right, 'value') else None
                    
                    if value is not None:
                        # Check if value is outside file bounds
                        if filter_expr.op == ">":
                            if value >= stats.get("max", float('inf')):
                                return True
                        elif filter_expr.op == "<":
                            if value <= stats.get("min", float('-inf')):
                                return True
                        elif filter_expr.op == "==":
                            if value < stats.get("min") or value > stats.get("max"):
                                return True
                                
        return False
        
    def get_stats(self) -> dict:
        """Get data skipping statistics"""
        total = self.files_skipped + self.files_scanned
        skip_rate = self.files_skipped / total if total > 0 else 0
        
        return {
            "files_skipped": self.files_skipped,
            "files_scanned": self.files_scanned,
            "skip_rate": skip_rate
        }
