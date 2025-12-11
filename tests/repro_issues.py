import os
import shutil
import pytest
import pyarrow as pa
from iceframe import IceFrame
from iceframe.expressions import col
from pyiceberg.catalog import load_catalog

# Setup a temporary catalog
CATALOG_DIR = "./tmp_catalog_repro"

@pytest.fixture
def clean_catalog():
    if os.path.exists(CATALOG_DIR):
        shutil.rmtree(CATALOG_DIR)
    os.makedirs(CATALOG_DIR)
    yield
    # shutil.rmtree(CATALOG_DIR)

def test_create_table_partition_spec(clean_catalog):
    """Test that create_table honors partition_spec"""
    config = {
        "type": "sql", 
        "uri": f"sqlite:///{CATALOG_DIR}/catalog.db",
        "warehouse": f"file://{os.path.abspath(CATALOG_DIR)}/warehouse"
    }
    ice = IceFrame(config)
    
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("dt", pa.string())
    ])
    
    # Define partition spec (needs PyIceberg PartitionField)
    # Since we can't easily construct raw PyIceberg specs in user code without internal knowledge,
    # let's assume the user passes a list of tuples or PyIceberg spec if they know how.
    # But wait, create_table signature says `partition_spec: Optional[List[tuple]]`.
    # PyIceberg create_table expects `PartitionSpec`.
    # Let's see if IceFrame converts it? No, in operations.py it just passed it through.
    # Wait, if `create_table` accepts `List[tuple]`, does PyIceberg accept that?
    # PyIceberg's `catalog.create_table` expects `partition_spec` to be `PartitionSpec`.
    # If IceFrame just passes a list, it might fail if PyIceberg doesn't support list.
    # Ah, checking PyIceberg docs/code... `create_table` arguments.
    # If IceFrame is just a wrapper, checking `operations.py`:
    # `def create_table(..., partition_spec=None, ...)`
    # It calls `self.catalog.create_table(..., partition_spec=partition_spec, ...)`
    
    # If I pass a verified PyIceberg PartitionSpec, it should work now.
    
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform
    
    # Construct a spec properly
    # Note: Constructing PartitionSpec manually is complex.
    # Let's try to pass it and see if it sticks.
    
    # For this test, we accept that the user must pass a valid object that PyIceberg accepts.
    # We just want to ensure it is PASSED to the function.
    
    # Let's mock the catalog to verify the call if we can't easily use a real one with complex specs.
    # But integration test is better.
    
    # Let's assume usage of PyIceberg PartitionSpec
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="dt_day")
    )
    
    table_name = "test_partitioned"
    ice.create_table("default.test_partitioned", schema, partition_spec=spec)
    
    # Verify
    tbl = ice.get_table("default.test_partitioned")
    assert tbl.spec() == spec
    print("SUCCESS: Partition spec was preserved!")

def test_read_table_pushdown(clean_catalog):
    """Test read_table with predicate pushdown"""
    config = {
        "type": "sql", 
        "uri": f"sqlite:///{CATALOG_DIR}/catalog_read.db",
        "warehouse": f"file://{os.path.abspath(CATALOG_DIR)}/warehouse_read"
    }
    ice = IceFrame(config)
    
    schema = {
        "id": "long",
        "category": "string"
    }
    
    ice.create_table("default.read_test", schema)
    
    # Add data
    import polars as pl
    data = pl.DataFrame({
        "id": [1, 2, 3],
        "category": ["A", "B", "A"]
    })
    ice.append_to_table("default.read_test", data)
    
    # Test pushdown
    # If pushdown works, we should be able to pass an Expression
    from iceframe.expressions import col
    
    df = ice.read_table("default.read_test", filter_expr=(col("category") == "B"))
    
    assert df.height == 1
    assert df["id"][0] == 2
    print("SUCCESS: Read table with Expression pushdown worked!")

if __name__ == "__main__":
    # Manually run if not using pytest CLI
    try:
        if os.path.exists(CATALOG_DIR):
            shutil.rmtree(CATALOG_DIR)
        os.makedirs(CATALOG_DIR)
        
        test_create_table_partition_spec(None)
        test_read_table_pushdown(None)
        print("ALL TESTS PASSED")
    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
