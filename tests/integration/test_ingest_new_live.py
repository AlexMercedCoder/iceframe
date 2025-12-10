import pytest
import polars as pl
import pandas as pd
import sqlite3
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env

@pytest.fixture
def ice():
    config = load_catalog_config_from_env()
    return IceFrame(config)

def test_new_ingestion_live(ice, tmp_path):
    # Test XML
    xml_path = tmp_path / "test.xml"
    df_pd = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    df_pd.to_xml(xml_path, index=False)
    
    table_name = "test_ingest_xml"
    ice.drop_table(table_name) if ice.table_exists(table_name) else None
    
    ice.create_table_from_xml(table_name, str(xml_path))
    # ice.append_to_table(table_name, pl.from_pandas(df_pd)) # create_table_from_xml already appends
    assert ice.table_exists(table_name)
    assert ice.read_table(table_name).height == 2
    ice.drop_table(table_name)

    # Test SQL (SQLite)
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    df_pd.to_sql("my_table", conn, index=False)
    conn.close()
    
    uri = f"sqlite:///{db_path}"
    query = "SELECT * FROM my_table"
    
    table_name = "test_ingest_sql"
    ice.drop_table(table_name) if ice.table_exists(table_name) else None
    
    ice.create_table_from_sql(table_name, query, uri)
    # ice.append_to_table(table_name, pl.from_pandas(df_pd)) # create_table_from_sql already appends
    assert ice.table_exists(table_name)
    assert ice.read_table(table_name).height == 2
    ice.drop_table(table_name)
