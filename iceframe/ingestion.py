"""
Data ingestion and bulk import.
"""

from typing import List, Optional
from pyiceberg.table import Table

class DataIngestion:
    """
    Manage data ingestion.
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def add_files(self, file_paths: List[str]) -> None:
        """
        Add existing data files to the table without rewriting.
        
        Args:
            file_paths: List of absolute paths to data files (Parquet/Avro/ORC)
        """
        try:
            if hasattr(self.table, "add_files"):
                # PyIceberg add_files API
                with self.table.add_files() as update:
                    for path in file_paths:
                        update.add_file(path)
            else:
                raise NotImplementedError("Adding files requires PyIceberg 0.6.0+")
        except AttributeError:
            raise NotImplementedError("Operation not supported by this PyIceberg version")
