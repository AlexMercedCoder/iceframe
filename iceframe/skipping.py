"""
Data skipping optimizations for IceFrame.
"""

from iceframe.expressions import BinaryExpression, Column, Expression


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
        # Check the predicate against the file's min/max bounds.
        #
        # These comparisons used to test ``filter_expr.op == ">"`` / ``"<"`` /
        # ``"=="``, but BinaryExpression stores its operator as ``"gt"`` /
        # ``"lt"`` / ``"eq"``. No branch could ever match, so no file was ever
        # skipped and the skipper was a no-op.
        if not isinstance(filter_expr, BinaryExpression):
            return False
        if not isinstance(filter_expr.left, Column):
            return False

        col_name = filter_expr.left.name
        if col_name not in file_stats:
            return False

        stats = file_stats[col_name]
        value = getattr(filter_expr.right, "value", None)
        if value is None:
            return False

        lo = stats.get("min")
        hi = stats.get("max")
        op = filter_expr.op

        # col > value  -> skip when every value in the file is <= value
        if op == "gt" and hi is not None:
            return hi <= value
        # col >= value -> skip when every value is < value
        if op == "ge" and hi is not None:
            return hi < value
        # col < value  -> skip when every value is >= value
        if op == "lt" and lo is not None:
            return lo >= value
        # col <= value -> skip when every value is > value
        if op == "le" and lo is not None:
            return lo > value
        # col == value -> skip when value falls outside [min, max]
        if op == "eq" and lo is not None and hi is not None:
            return value < lo or value > hi

        return False

    def record(self, skipped: bool) -> None:
        """Record the outcome of one file-level skip decision."""
        if skipped:
            self.files_skipped += 1
        else:
            self.files_scanned += 1

    def get_stats(self) -> dict:
        """Get data skipping statistics"""
        total = self.files_skipped + self.files_scanned
        skip_rate = self.files_skipped / total if total > 0 else 0

        return {
            "files_skipped": self.files_skipped,
            "files_scanned": self.files_scanned,
            "skip_rate": skip_rate
        }
