"""
Data ingestion and bulk import.
"""

from pyiceberg.table import Table


class DataIngestion:
    """
    Manage data ingestion.
    """

    def __init__(self, table: Table):
        self.table = table

    def add_files(self, file_paths: list[str]) -> None:
        """
        Add existing data files to the table without rewriting.

        Args:
            file_paths: List of absolute paths to data files (Parquet/Avro/ORC)
        """
        if not file_paths:
            raise ValueError("file_paths must contain at least one file")
        try:
            self.table.add_files(list(file_paths))
        except AttributeError as exc:
            raise NotImplementedError("Operation not supported by this PyIceberg version") from exc
