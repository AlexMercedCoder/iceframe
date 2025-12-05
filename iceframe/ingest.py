"""
Data ingestion module for IceFrame.

Provides utilities to read data from various sources into Polars DataFrames,
which can then be used to create Iceberg tables.
"""

from typing import Any, Optional, Dict, List, Union
import polars as pl
import pyarrow as pa

def read_delta(path: str, version: Optional[int] = None, **kwargs) -> pl.DataFrame:
    """
    Read a Delta Lake table into a Polars DataFrame.
    
    Args:
        path: Path to the Delta table
        version: Optional version to read
        **kwargs: Additional arguments passed to pl.read_delta
        
    Returns:
        Polars DataFrame
    """
    try:
        return pl.read_delta(path, version=version, **kwargs)
    except ImportError:
        raise ImportError("deltalake is required. Install with 'pip install iceframe[delta]'")

def read_lance(path: str, **kwargs) -> pl.DataFrame:
    """
    Read a Lance dataset into a Polars DataFrame.
    
    Args:
        path: Path to the Lance dataset
        **kwargs: Additional arguments passed to lance.dataset
        
    Returns:
        Polars DataFrame
    """
    try:
        import lance
        ds = lance.dataset(path, **kwargs)
        # Convert to Arrow Table then Polars
        return pl.from_arrow(ds.to_table())
    except ImportError:
        raise ImportError("pylance is required. Install with 'pip install iceframe[lance]'")

def read_vortex(path: str, **kwargs) -> pl.DataFrame:
    """
    Read a Vortex file into a Polars DataFrame.
    
    Args:
        path: Path to the Vortex file
        **kwargs: Additional arguments
        
    Returns:
        Polars DataFrame
    """
    try:
        import vortex
        # Assuming vortex.open().scan().read_all() returns an Arrow-compatible object or similar
        # Based on research: vortex.open("example.vortex").scan().read_all()
        # We need to verify what read_all() returns. It likely returns a Vortex Array which might support to_arrow()
        
        vortex_array = vortex.open(path).scan().read_all()
        
        # Check if it has to_arrow() or similar
        if hasattr(vortex_array, "to_arrow"):
            return pl.from_arrow(vortex_array.to_arrow())
        else:
            # Fallback or error if we can't convert
            # Maybe it returns a PyArrow table directly?
            # Let's assume it supports Arrow conversion as it's a columnar format
            return pl.from_arrow(vortex_array.to_arrow())
            
    except ImportError:
        raise ImportError("vortex-data is required. Install with 'pip install iceframe[vortex]'")
    except Exception as e:
        raise ValueError(f"Failed to read Vortex file: {e}")

def read_excel(path: str, sheet_name: str = "Sheet1", **kwargs) -> pl.DataFrame:
    """
    Read an Excel file into a Polars DataFrame.
    
    Args:
        path: Path to the Excel file
        sheet_name: Name of the sheet to read
        **kwargs: Additional arguments passed to pl.read_excel
        
    Returns:
        Polars DataFrame
    """
    try:
        return pl.read_excel(path, sheet_name=sheet_name, **kwargs)
    except ImportError:
        raise ImportError("fastexcel is required. Install with 'pip install iceframe[excel]'")

def read_gsheets(url: str, credentials: Any = None, sheet_name: Optional[str] = None, **kwargs) -> pl.DataFrame:
    """
    Read a Google Sheet into a Polars DataFrame.
    
    Args:
        url: URL of the Google Sheet
        credentials: Path to service account JSON or credentials object
        sheet_name: Optional name of the worksheet. If None, reads the first sheet.
        **kwargs: Additional arguments
        
    Returns:
        Polars DataFrame
    """
    try:
        import gspread
        
        if isinstance(credentials, str):
            gc = gspread.service_account(filename=credentials)
        elif credentials:
            gc = gspread.authorize(credentials)
        else:
            # Try default auth? Or raise error?
            # gspread usually requires explicit auth or config file
            gc = gspread.service_account() # Looks for default config
            
        sh = gc.open_by_url(url)
        
        if sheet_name:
            worksheet = sh.worksheet(sheet_name)
        else:
            worksheet = sh.sheet1
            
        # Get all values
        data = worksheet.get_all_records()
        
        return pl.DataFrame(data)
        
    except ImportError:
        raise ImportError("gspread is required. Install with 'pip install iceframe[gsheets]'")

def read_hudi(path: str, **kwargs) -> pl.DataFrame:
    """
    Read a Hudi table into a Polars DataFrame using Daft.
    
    Args:
        path: Path to the Hudi table
        **kwargs: Additional arguments passed to daft.read_hudi
        
    Returns:
        Polars DataFrame
    """
    try:
        import daft
        df = daft.read_hudi(path, **kwargs)
        # Convert Daft DataFrame to Arrow then Polars
        return pl.from_arrow(df.to_arrow())
    except ImportError:
        raise ImportError("getdaft is required. Install with 'pip install iceframe[hudi]'")
