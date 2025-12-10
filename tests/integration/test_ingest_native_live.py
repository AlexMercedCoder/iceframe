import pytest
import polars as pl
import os
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env

@pytest.fixture
def ice():
    config = load_catalog_config_from_env()
    return IceFrame(config)

@pytest.fixture
def temp_files(tmp_path):
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    
    csv_path = tmp_path / "test.csv"
    df.write_csv(csv_path)
    
    json_path = tmp_path / "test.json"
    df.write_json(json_path)
    
    parquet_path = tmp_path / "test.parquet"
    df.write_parquet(parquet_path)
    
    ipc_path = tmp_path / "test.ipc"
    df.write_ipc(ipc_path)
    
    # Avro/ORC might require extra setup or libraries in some envs, skipping for basic live test if not critical
    # But we should test if supported
    
    return {
        "csv": str(csv_path),
        "json": str(json_path),
        "parquet": str(parquet_path),
        "ipc": str(ipc_path),
        "df": df
    }

def test_native_ingestion_live(ice, temp_files):
    # Test CSV
    table_name = "test_ingest_csv"
    ice.drop_table(table_name) if ice.table_exists(table_name) else None
    
    ice.create_table(table_name, temp_files["df"])
    ice.append_to_table(table_name, temp_files["df"]) # Append initial data
    print(f"Initial count: {ice.read_table(table_name).height}")
    ice.insert_from_file(table_name, temp_files["csv"], format="csv")
    print(f"Count after insert: {ice.read_table(table_name).height}")
    assert ice.table_exists(table_name)
    assert ice.read_table(table_name).height == 6 # 3 initial + 3 inserted
    ice.drop_table(table_name)

    # Test Parquet (Create Table)
    table_name = "test_ingest_parquet"
    ice.drop_table(table_name) if ice.table_exists(table_name) else None
    
    ice.create_table_from_parquet(table_name, temp_files["parquet"])
    assert ice.table_exists(table_name)
    assert ice.read_table(table_name).height == 3
    ice.drop_table(table_name)
    
    # Test Inferred Insert
    table_name = "test_ingest_inferred"
    ice.drop_table(table_name) if ice.table_exists(table_name) else None
    
    # Create empty table first
    ice.create_table(table_name, temp_files["df"])
    
    ice.insert_from_file(table_name, temp_files["json"]) # Should infer json
    assert ice.read_table(table_name).height == 3
    ice.drop_table(table_name)
