import pytest
import polars as pl
from iceframe.core import IceFrame
from iceframe.utils import load_catalog_config_from_env

@pytest.fixture
def ice():
    config = load_catalog_config_from_env()
    return IceFrame(config)

@pytest.fixture
def temp_table(ice):
    table_name = "test_viz_live"
    try:
        ice.drop_table(table_name)
    except Exception:
        pass
        
    schema = {"id": "long", "category": "string", "value": "double"}
    ice.create_table(table_name, schema)
    
    data = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "category": ["A", "A", "B", "B"],
        "value": [10.0, 20.0, 30.0, 40.0]
    })
    ice.append_to_table(table_name, data)
    
    yield table_name
    
    try:
        ice.drop_table(table_name)
    except Exception:
        pass

def test_visualization_live(ice, temp_table):
    # Just verify it runs without error and returns a Chart object
    import altair as alt
    
    chart = ice.viz.plot_bar(temp_table, "category", "value")
    assert isinstance(chart, alt.Chart)
    
    chart = ice.viz.plot_distribution(temp_table, "value")
    assert isinstance(chart, alt.Chart)
