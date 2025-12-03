"""
Unit tests for Query Builder API
"""

import pytest
import polars as pl
import datetime
from iceframe.expressions import col, lit
from iceframe.functions import count, sum, avg, min, max, row_number, rank, dense_rank, when


def test_query_select_filter(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test basic select and filter"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Query: SELECT id, name FROM table WHERE id > 2
    df = (ice_frame.query(test_table_name)
          .select("id", "name")
          .filter(col("id") > 2)
          .execute())
    
    assert len(df) == 3
    assert set(df.columns) == {"id", "name"}
    assert df["id"].min() == 3


def test_query_group_by_agg(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test group by and aggregation"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add data with groups
    data = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["A", "A", "B", "B", "C"],
        "age": [20, 30, 40, 50, 60],
        "created_at": [datetime.datetime.now()] * 5
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    
    ice_frame.append_to_table(test_table_name, data)
    
    # Query: SELECT name, count(id), avg(age) FROM table GROUP BY name
    df = (ice_frame.query(test_table_name)
          .select(
              col("name"),
              count(col("id")).alias("count"),
              avg(col("age")).alias("avg_age")
          )
          .group_by("name")
          .execute())
    
    assert len(df) == 3
    
    # Check group A
    group_a = df.filter(pl.col("name") == "A")
    assert group_a["count"][0] == 2
    assert group_a["avg_age"][0] == 25.0


def test_query_window_functions(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test window functions"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add data
    data = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["A", "A", "B", "B", "C"],
        "age": [20, 30, 40, 50, 60],
        "created_at": [datetime.datetime.now()] * 5
    }).with_columns(pl.col("age").cast(pl.Int32))
    
    ice_frame.append_to_table(test_table_name, data)
    
    # Query: SELECT *, row_number() OVER (PARTITION BY name ORDER BY age) as rn
    df = (ice_frame.query(test_table_name)
          .select(
              col("name"),
              col("age"),
              row_number().over(partition_by=col("name"), order_by=col("age")).alias("rn")
          )
          .execute())
    
    # Check group A
    group_a = df.filter(pl.col("name") == "A").sort("age")
    assert group_a["rn"].to_list() == [1, 2]


def test_query_case_when(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test case when expression"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    data = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "age": [20, 30, 40],
        "created_at": [datetime.datetime.now()] * 3
    }).with_columns(pl.col("age").cast(pl.Int32))
    
    ice_frame.append_to_table(test_table_name, data)
    
    # Query: SELECT age, CASE WHEN age < 30 THEN 'Young' ELSE 'Old' END as category
    df = (ice_frame.query(test_table_name)
          .select(
              col("age"),
              when(col("age") < 30, "Young")
              .otherwise("Old")
              .alias("category")
          )
          .execute())
    
    assert df.filter(pl.col("age") == 20)["category"][0] == "Young"
    assert df.filter(pl.col("age") == 30)["category"][0] == "Old"


def test_query_update(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test update operation"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    data = pl.DataFrame({
        "id": [1, 2],
        "name": ["A", "B"],
        "age": [20, 30],
        "created_at": [datetime.datetime.now()] * 2
    }).with_columns(pl.col("age").cast(pl.Int32))
    
    ice_frame.append_to_table(test_table_name, data)
    
    # Update: UPDATE table SET age = 25 WHERE id = 1
    (ice_frame.query(test_table_name)
     .filter(col("id") == 1)
     .update({"age": 25}))
    
    # Verify
    df = ice_frame.read_table(test_table_name)
    assert df.filter(pl.col("id") == 1)["age"][0] == 25
    assert df.filter(pl.col("id") == 2)["age"][0] == 30


def test_query_delete(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test delete operation"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    data = pl.DataFrame({
        "id": [1, 2],
        "name": ["A", "B"],
        "age": [20, 30],
        "created_at": [datetime.datetime.now()] * 2
    }).with_columns(pl.col("age").cast(pl.Int32))
    
    ice_frame.append_to_table(test_table_name, data)
    
    # Delete: DELETE FROM table WHERE id = 1
    try:
        (ice_frame.query(test_table_name)
         .filter(col("id") == 1)
         .delete())
        
        # Verify
        df = ice_frame.read_table(test_table_name)
        assert len(df) == 1
        assert df["id"][0] == 2
    except (NotImplementedError, AttributeError):
        pytest.skip("Delete not supported by catalog")


def test_query_merge(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test merge operation"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Target data
    target = pl.DataFrame({
        "id": [1, 2],
        "name": ["A", "B"],
        "age": [20, 30],
        "created_at": [datetime.datetime.now()] * 2
    }).with_columns(pl.col("age").cast(pl.Int32))
    
    ice_frame.append_to_table(test_table_name, target)
    
    # Source data (1 update, 1 insert)
    source = pl.DataFrame({
        "id": [2, 3],
        "name": ["B_updated", "C"],
        "age": [35, 40],
        "created_at": [datetime.datetime.now()] * 2
    }).with_columns(pl.col("age").cast(pl.Int32))
    
    # Merge
    (ice_frame.query(test_table_name)
     .merge(
         source_data=source,
         on="id",
         when_matched_update={"name": "name", "age": "age"}, # Update all cols
         when_not_matched_insert={"id": "id", "name": "name", "age": "age"} # Insert all cols
     ))
    
    # Verify
    df = ice_frame.read_table(test_table_name).sort("id")
    assert len(df) == 3
    
    # Check update (id=2)
    row_2 = df.filter(pl.col("id") == 2)
    # Note: Our simple merge implementation replaces the row, so name should be B_updated
    # But wait, our implementation logic was:
    # df_update = source_data.join(target_df.select(on), on=on, how="semi")
    # So it takes the source row completely.
    assert row_2["name"][0] == "B_updated"
    assert row_2["age"][0] == 35
    
    # Check insert (id=3)
    row_3 = df.filter(pl.col("id") == 3)
    assert row_3["name"][0] == "C"
    
    # Check keep (id=1)
    row_1 = df.filter(pl.col("id") == 1)
    assert row_1["name"][0] == "A"
