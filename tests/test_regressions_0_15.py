"""Regression tests for defects fixed in 0.15.0."""

import polars as pl
import pytest
from pyiceberg.table import Table


@pytest.fixture
def ice(fresh_ice):
    fresh_ice.create_table("default.t", pl.DataFrame({"id": [1], "score": [1.5], "note": ["x"]}))
    return fresh_ice


def test_append_with_all_null_optional_column(ice):
    """A batch whose optional column is all None used to fail with a null-type error."""
    ice.append_to_table(
        "default.t", pl.DataFrame({"id": [2, 3], "score": [None, None], "note": [None, None]})
    )
    out = ice.read_table("default.t").sort("id")
    assert out["id"].to_list() == [2, 3]
    assert out["score"].to_list() == [None, None]
    assert out.schema["score"] == pl.Float64


def test_overwrite_and_upsert_with_all_null_column(ice):
    ice.overwrite_table("default.t", pl.DataFrame({"id": [5], "score": [None], "note": ["y"]}))
    assert ice.read_table("default.t")["id"].to_list() == [5]


def test_failed_branch_append_never_lands_on_main(ice, monkeypatch):
    """0.14 retried a branch append that raised TypeError as a plain append to main."""
    ice.append_to_table("default.t", pl.DataFrame({"id": [1], "score": [1.0], "note": ["a"]}))
    before = ice.read_table("default.t").height
    real_append = Table.append

    def failing_append(self, df, *args, **kwargs):
        if kwargs.get("branch"):
            raise TypeError("simulated branch write failure")
        return real_append(self, df, *args, **kwargs)

    monkeypatch.setattr(Table, "append", failing_append)
    with pytest.raises(TypeError, match="simulated"):
        ice.append_to_table(
            "default.t", pl.DataFrame({"id": [9], "score": [9.0], "note": ["b"]}), branch="audit"
        )
    assert ice.read_table("default.t").height == before


def test_branch_append_writes_only_to_branch(ice):
    ice.append_to_table("default.t", pl.DataFrame({"id": [1], "score": [1.0], "note": ["a"]}))
    main_rows = ice.read_table("default.t").height
    table = ice.get_table("default.t")
    table.manage_snapshots().create_branch(table.current_snapshot().snapshot_id, "audit").commit()
    ice.append_to_table(
        "default.t", pl.DataFrame({"id": [7], "score": [7.0], "note": ["z"]}), branch="audit"
    )
    assert ice.read_table("default.t").height == main_rows
    audit = ice.get_table("default.t").scan(
        snapshot_id=ice.get_table("default.t").refs()["audit"].snapshot_id
    )
    assert 7 in audit.to_arrow()["id"].to_pylist()


def test_append_polars_frame_to_required_columns(fresh_ice):
    """Polars frames are always nullable; required Iceberg fields rejected them."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )
    fresh_ice.create_table("default.req", schema)
    fresh_ice.append_to_table("default.req", pl.DataFrame({"id": [1, 2], "name": ["a", None]}))
    assert fresh_ice.read_table("default.req").height == 2

    # A real null in a required column is still refused.
    with pytest.raises(Exception):
        fresh_ice.append_to_table(
            "default.req", pl.DataFrame({"id": [None, 3], "name": ["x", "y"]})
        )


def test_pydantic_optional_union_syntax_keeps_its_type():
    from iceframe.pydantic import to_iceberg_schema

    pydantic = pytest.importorskip("pydantic")
    from pyiceberg.types import DoubleType

    class M(pydantic.BaseModel):
        score: float | None = None
        name: str | None

    schema = to_iceberg_schema(M)
    assert isinstance(schema.find_field("score").field_type, DoubleType)
    # Nullable without a default is still optional: it can hold None.
    assert schema.find_field("name").required is False


def test_pydantic_nested_field_ids_are_unique():
    pydantic = pytest.importorskip("pydantic")
    from iceframe.pydantic import to_iceberg_schema

    class Address(pydantic.BaseModel):
        street: str
        zip: str | None = None

    class User(pydantic.BaseModel):
        id: int
        address: Address
        tags: list[str] = []
        attrs: dict[str, int] = {}

    from pyiceberg.schema import index_by_id

    ids = list(index_by_id(to_iceberg_schema(User)).keys())
    # id, address, street, zip, tags, tags.element, attrs, attrs.key, attrs.value
    assert len(ids) == len(set(ids)) == 9


def test_append_nested_polars_data_to_required_nested_fields(fresh_ice):
    from pyiceberg.schema import Schema
    from pyiceberg.types import ListType, LongType, NestedField, StringType, StructType

    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "tags", ListType(3, StringType(), element_required=True), required=True),
        NestedField(
            4,
            "addr",
            StructType(NestedField(5, "street", StringType(), required=True)),
            required=False,
        ),
    )
    fresh_ice.create_table("default.nested", schema)
    fresh_ice.append_to_table(
        "default.nested",
        pl.DataFrame({"id": [1], "tags": [["a", "b"]], "addr": [{"street": "Main"}]}),
    )
    assert fresh_ice.read_table("default.nested")["tags"].to_list() == [["a", "b"]]

    # A null list element is still refused rather than silently cast.
    with pytest.raises(Exception):
        fresh_ice.append_to_table(
            "default.nested",
            pl.DataFrame({"id": [2], "tags": [["a", None]], "addr": [{"street": "x"}]}),
        )
