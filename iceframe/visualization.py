"""
Visualization module for IceFrame using Altair.
"""

from typing import Optional, List, Union, Any
import polars as pl

try:
    import altair as alt
    VIZ_AVAILABLE = True
except ImportError:
    VIZ_AVAILABLE = False

class Visualizer:
    """
    Generate visualizations from Iceberg tables.
    """
    
    def __init__(self, ice_frame):
        """
        Initialize Visualizer.
        
        Args:
            ice_frame: IceFrame instance
        """
        if not VIZ_AVAILABLE:
            raise ImportError("altair is required. Install with 'pip install iceframe[viz]'")
            
        self.ice_frame = ice_frame
        
    def _get_data(self, table_name: str, limit: int = 10000) -> pl.DataFrame:
        """
        Get data for visualization (limited to prevent browser crash).
        
        Args:
            table_name: Name of the table
            limit: Max rows to fetch
            
        Returns:
            Polars DataFrame
        """
        # Use query builder or read_table with limit
        # Since read_table doesn't support limit natively in PyIceberg scan (it does but we wrap it),
        # let's use scan_batches or just read and slice (if small) or query.
        # Query is best if we want to push down limit.
        
        # Using scan_batches to get iterator and take first batch(es) up to limit is efficient.
        batches = self.ice_frame._operations.scan_batches(table_name, limit=limit)
        
        # Collect batches
        dfs = []
        count = 0
        for batch in batches:
            df = pl.from_arrow(batch)
            dfs.append(df)
            count += df.height
            if count >= limit:
                break
                
        if not dfs:
            return pl.DataFrame()
            
        return pl.concat(dfs).head(limit)

    def plot_distribution(self, table_name: str, column: str, limit: int = 10000) -> 'alt.Chart':
        """
        Plot distribution of a column (Histogram).
        
        Args:
            table_name: Table name
            column: Column to plot
            limit: Max rows
        """
        df = self._get_data(table_name, limit)
        return alt.Chart(df.to_pandas()).mark_bar().encode(
            alt.X(column, bin=True),
            y='count()'
        ).properties(title=f"Distribution of {column} in {table_name}")

    def plot_scatter(
        self, table_name: str, x: str, y: str, color: Optional[str] = None, limit: int = 10000
    ) -> 'alt.Chart':
        """
        Plot scatter plot.
        """
        df = self._get_data(table_name, limit)
        chart = alt.Chart(df.to_pandas()).mark_circle().encode(
            x=x,
            y=y,
            tooltip=[x, y]
        )
        
        if color:
            chart = chart.encode(color=color)
            
        return chart.properties(title=f"{x} vs {y} in {table_name}")

    def plot_bar(
        self, table_name: str, x: str, y: str, limit: int = 10000
    ) -> 'alt.Chart':
        """
        Plot bar chart.
        """
        df = self._get_data(table_name, limit)
        return alt.Chart(df.to_pandas()).mark_bar().encode(
            x=x,
            y=y,
            tooltip=[x, y]
        ).properties(title=f"{y} by {x} in {table_name}")

    def plot_line(
        self, table_name: str, x: str, y: str, color: Optional[str] = None, limit: int = 10000
    ) -> 'alt.Chart':
        """
        Plot line chart.
        """
        df = self._get_data(table_name, limit)
        chart = alt.Chart(df.to_pandas()).mark_line().encode(
            x=x,
            y=y,
            tooltip=[x, y]
        )
        
        if color:
            chart = chart.encode(color=color)
            
        return chart.properties(title=f"{y} over {x} in {table_name}")
