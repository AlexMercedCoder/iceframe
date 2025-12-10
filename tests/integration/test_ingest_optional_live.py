import pytest
import polars as pl
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env

@pytest.fixture
def ice():
    config = load_catalog_config_from_env()
    return IceFrame(config)

def test_optional_ingestion_live_skip_if_missing(ice, tmp_path):
    # This test will likely fail or require skipping if dependencies aren't installed.
    # We'll try to use Excel as it's common, but we need fastexcel.
    
    try:
        import fastexcel
        import xlsxwriter
    except ImportError:
        pytest.skip("fastexcel or xlsxwriter not installed")

    # Create a dummy Excel file
    excel_path = tmp_path / "test.xlsx"
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    df.write_excel(excel_path)
    
    table_name = "test_ingest_excel"
    ice.drop_table(table_name) if ice.table_exists(table_name) else None
    
    ice.create_table_from_excel(table_name, str(excel_path))
    assert ice.table_exists(table_name)
    # create_table_from_excel calls append_to_table internally, so data should be there
    assert ice.read_table(table_name).height == 2
    
    ice.drop_table(table_name)
