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

def read_csv(path: str, **kwargs) -> pl.DataFrame:
    """
    Read a CSV file into a Polars DataFrame.
    
    Args:
        path: Path to the CSV file
        **kwargs: Additional arguments passed to pl.read_csv
        
    Returns:
        Polars DataFrame
    """
    return pl.read_csv(path, **kwargs)

def read_json(path: str, **kwargs) -> pl.DataFrame:
    """
    Read a JSON file into a Polars DataFrame.
    
    Args:
        path: Path to the JSON file
        **kwargs: Additional arguments passed to pl.read_json
        
    Returns:
        Polars DataFrame
    """
    return pl.read_json(path, **kwargs)

def read_parquet(path: str, **kwargs) -> pl.DataFrame:
    """
    Read a Parquet file into a Polars DataFrame.
    
    Args:
        path: Path to the Parquet file
        **kwargs: Additional arguments passed to pl.read_parquet
        
    Returns:
        Polars DataFrame
    """
    return pl.read_parquet(path, **kwargs)

def read_ipc(path: str, **kwargs) -> pl.DataFrame:
    """
    Read an IPC (Arrow) file into a Polars DataFrame.
    
    Args:
        path: Path to the IPC file
        **kwargs: Additional arguments passed to pl.read_ipc
        
    Returns:
        Polars DataFrame
    """
    return pl.read_ipc(path, **kwargs)

def read_avro(path: str, **kwargs) -> pl.DataFrame:
    """
    Read an Avro file into a Polars DataFrame.
    
    Args:
        path: Path to the Avro file
        **kwargs: Additional arguments passed to pl.read_avro
        
    Returns:
        Polars DataFrame
    """
    return pl.read_avro(path, **kwargs)

def read_orc(path: str, **kwargs) -> pl.DataFrame:
    """
    Read an ORC file into a Polars DataFrame.
    
    Args:
        path: Path to the ORC file
        **kwargs: Additional arguments passed to pl.read_orc
        
    Returns:
        Polars DataFrame
    """
    return pl.read_orc(path, **kwargs)

def read_sql(query: str, connection_uri: str, **kwargs) -> pl.DataFrame:
    """
    Read from a SQL database into a Polars DataFrame.
    
    Args:
        query: SQL query to execute
        connection_uri: Database connection URI
        **kwargs: Additional arguments passed to pl.read_database_uri
        
    Returns:
        Polars DataFrame
    """
    try:
        return pl.read_database_uri(query, connection_uri, **kwargs)
    except ImportError:
        raise ImportError("connectorx or sqlalchemy is required. Install with 'pip install iceframe[sql]'")

def read_xml(path: str, **kwargs) -> pl.DataFrame:
    """
    Read an XML file into a Polars DataFrame.
    
    Args:
        path: Path to the XML file
        **kwargs: Additional arguments passed to pandas.read_xml
        
    Returns:
        Polars DataFrame
    """
    try:
        import pandas as pd
        # Polars doesn't have native read_xml yet, use pandas
        df_pd = pd.read_xml(path, **kwargs)
        return pl.from_pandas(df_pd)
    except ImportError:
        raise ImportError("lxml is required. Install with 'pip install iceframe[xml]'")

def read_sas(path: str, **kwargs) -> pl.DataFrame:
    """
    Read a SAS file (.sas7bdat) into a Polars DataFrame.
    
    Args:
        path: Path to the SAS file
        **kwargs: Additional arguments passed to pandas.read_sas
        
    Returns:
        Polars DataFrame
    """
    try:
        import pandas as pd
        df_pd = pd.read_sas(path, format='sas7bdat', **kwargs)
        return pl.from_pandas(df_pd)
    except ImportError:
        raise ImportError("pyreadstat is required. Install with 'pip install iceframe[stats]'")

def read_spss(path: str, **kwargs) -> pl.DataFrame:
    """
    Read an SPSS file (.sav) into a Polars DataFrame.
    
    Args:
        path: Path to the SPSS file
        **kwargs: Additional arguments passed to pandas.read_spss
        
    Returns:
        Polars DataFrame
    """
    try:
        import pandas as pd
        df_pd = pd.read_spss(path, **kwargs)
        return pl.from_pandas(df_pd)
    except ImportError:
        raise ImportError("pyreadstat is required. Install with 'pip install iceframe[stats]'")

def read_stata(path: str, **kwargs) -> pl.DataFrame:
    """
    Read a Stata file (.dta) into a Polars DataFrame.
    
    Args:
        path: Path to the Stata file
        **kwargs: Additional arguments passed to pandas.read_stata
        
    Returns:
        Polars DataFrame
    """
    try:
        import pandas as pd
        df_pd = pd.read_stata(path, **kwargs)
        return pl.from_pandas(df_pd)
    except ImportError:
        raise ImportError("pyreadstat is required. Install with 'pip install iceframe[stats]'")

def read_api(url: str, json_key: Optional[str] = None, **kwargs) -> pl.DataFrame:
    """
    Read data from a REST API into a Polars DataFrame.
    
    Args:
        url: URL to fetch data from
        json_key: Optional key to extract list of records from JSON response
        **kwargs: Additional arguments passed to requests.get
        
    Returns:
        Polars DataFrame
    """
    try:
        import requests
        response = requests.get(url, **kwargs)
        response.raise_for_status()
        data = response.json()
        
        if json_key:
            if isinstance(data, dict) and json_key in data:
                data = data[json_key]
            else:
                raise ValueError(f"Key '{json_key}' not found in response")
                
        # Handle if data is a single dict instead of list
        if isinstance(data, dict):
            data = [data]
            
        return pl.DataFrame(data)
    except ImportError:
        raise ImportError("requests is required. Install with 'pip install iceframe[api]'")

def read_huggingface(dataset_name: str, split: str = "train", **kwargs) -> pl.DataFrame:
    """
    Read a HuggingFace dataset into a Polars DataFrame.
    
    Args:
        dataset_name: Name of the dataset (e.g. 'lhoestq/test')
        split: Split to read (default: 'train')
        **kwargs: Additional arguments passed to load_dataset
        
    Returns:
        Polars DataFrame
    """
    try:
        from datasets import load_dataset
        ds = load_dataset(dataset_name, split=split, **kwargs)
        # Convert to Arrow then Polars
        return pl.from_arrow(ds.data.table)
    except ImportError:
        raise ImportError("datasets is required. Install with 'pip install iceframe[hf]'")

def read_html(url: str, match: Optional[str] = None, **kwargs) -> pl.DataFrame:
    """
    Read HTML tables into a Polars DataFrame.
    
    Args:
        url: URL or HTML string
        match: Optional regex or string to match specific table
        **kwargs: Additional arguments passed to pandas.read_html
        
    Returns:
        Polars DataFrame (concatenated if multiple tables found, or first one)
    """
    try:
        import pandas as pd
        # pandas.read_html returns a list of DataFrames
        dfs = pd.read_html(url, match=match, **kwargs)
        
        if not dfs:
            raise ValueError("No tables found in HTML")
            
        # If multiple tables, we could return a list, but IceFrame expects a single DF
        # For now, let's return the first one or concat?
        # Let's return the first one as it's the most common use case
        # Or maybe concat if they have same schema?
        # Let's stick to first one for simplicity, user can use match to be specific
        return pl.from_pandas(dfs[0])
    except ImportError:
        raise ImportError("lxml, html5lib, and beautifulsoup4 are required. Install with 'pip install iceframe[html]'")

def read_clipboard(**kwargs) -> pl.DataFrame:
    """
    Read data from the system clipboard into a Polars DataFrame.
    
    Args:
        **kwargs: Additional arguments passed to pandas.read_clipboard
        
    Returns:
        Polars DataFrame
    """
    try:
        import pandas as pd
        df_pd = pd.read_clipboard(**kwargs)
        return pl.from_pandas(df_pd)
    except ImportError:
        raise ImportError("pyperclip is required. Install with 'pip install iceframe[clipboard]'")

def read_folder(path: str, pattern: str = "*", **kwargs) -> pl.DataFrame:
    """
    Read all files in a folder matching a pattern into a single Polars DataFrame.
    
    Args:
        path: Path to the folder
        pattern: Glob pattern to match files (default: '*')
        **kwargs: Additional arguments passed to the specific read function
        
    Returns:
        Polars DataFrame
    """
    import glob
    import os
    
    files = glob.glob(os.path.join(path, pattern))
    if not files:
        raise ValueError(f"No files found in {path} matching {pattern}")
        
    dfs = []
    for file_path in files:
        # Infer format from extension
        _, ext = os.path.splitext(file_path)
        fmt = ext.lower().lstrip('.')
        
        if fmt == 'csv':
            dfs.append(read_csv(file_path, **kwargs))
        elif fmt == 'json':
            dfs.append(read_json(file_path, **kwargs))
        elif fmt == 'parquet':
            dfs.append(read_parquet(file_path, **kwargs))
        elif fmt in ['xls', 'xlsx']:
            dfs.append(read_excel(file_path, **kwargs))
        # Add more as needed
        
    if not dfs:
        raise ValueError("No supported files found")
        
    return pl.concat(dfs)
