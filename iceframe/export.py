"""
Data export functionality
"""

from typing import Optional
import polars as pl


class DataExporter:
    """Handle data export operations"""
    
    def to_parquet(
        self,
        df: pl.DataFrame,
        output_path: str,
        compression: str = "snappy",
    ) -> None:
        """
        Export DataFrame to Parquet file.
        
        Args:
            df: Polars DataFrame to export
            output_path: Path to output file
            compression: Compression codec (snappy, gzip, lz4, zstd)
        """
        df.write_parquet(output_path, compression=compression)
    
    def to_csv(
        self,
        df: pl.DataFrame,
        output_path: str,
        separator: str = ",",
        include_header: bool = True,
    ) -> None:
        """
        Export DataFrame to CSV file.
        
        Args:
            df: Polars DataFrame to export
            output_path: Path to output file
            separator: Field separator
            include_header: Whether to include header row
        """
        df.write_csv(
            output_path,
            separator=separator,
            include_header=include_header,
        )
    
    def to_json(
        self,
        df: pl.DataFrame,
        output_path: str,
        pretty: bool = False,
    ) -> None:
        """
        Export DataFrame to JSON file.
        
        Args:
            df: Polars DataFrame to export
            output_path: Path to output file
            pretty: Whether to pretty-print JSON (Note: ignored in current Polars version)
        """
        # Polars write_json doesn't support pretty printing directly in all versions
        df.write_json(output_path)
