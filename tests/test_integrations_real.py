"""
Optional integrations exercised against the real libraries.

The unit tests for these modules replace DataFusion, Ray and Altair with mocks,
which is how 0.14.0 shipped a DataFusion integration that failed on every
query. Every test here imports the real package (skipping when it is not
installed) and runs against a real local catalog. CI installs all of them in
the ``integrations`` job.

Kafka tests also need a broker: set ``ICEFRAME_TEST_KAFKA_BOOTSTRAP``.
"""

import json
import os
import sqlite3
import time
import uuid

import polars as pl
import pyarrow as pa
import pytest

from iceframe import IceFrame, ingest
from tests.conftest import make_local_catalog_config


@pytest.fixture
def rows():
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["a", "b", "c", "d"],
            "v": [1.5, 2.5, 3.5, 4.5],
        }
    )


@pytest.fixture
def config(tmp_path):
    return make_local_catalog_config(str(tmp_path))


@pytest.fixture
def ice(config, rows):
    ice = IceFrame(config)
    ice.create_namespace("default")
    ice.create_namespace("sales")
    ice.create_table("default.t", rows)
    ice.append_to_table("default.t", rows)
    ice.create_table("sales.orders", rows)
    ice.append_to_table("sales.orders", rows.filter(pl.col("id") <= 2))
    return ice


# --------------------------------------------------------------------------
# DataFusion
# --------------------------------------------------------------------------


def test_datafusion_namespace_qualified_query(ice):
    pytest.importorskip("datafusion")
    out = ice.query_datafusion(
        "SELECT count(*) AS n, sum(v) AS s FROM default.t", tables=["default.t"]
    )
    assert out.to_dicts() == [{"n": 4, "s": 12.0}]


def test_datafusion_auto_registers_referenced_tables(ice):
    pytest.importorskip("datafusion")
    out = ice.query_datafusion(
        "SELECT t.name, o.v FROM default.t t JOIN sales.orders o ON t.id = o.id ORDER BY t.id"
    )
    assert out.to_dicts() == [{"name": "a", "v": 1.5}, {"name": "b", "v": 2.5}]


def test_datafusion_bare_names_and_alias(ice):
    pytest.importorskip("datafusion")
    from iceframe.datafusion_ops import DataFusionManager

    dfm = DataFusionManager(ice)
    dfm.register_table("default.t")
    dfm.register_table("sales.orders", alias="o")
    assert dfm.query("SELECT count(*) AS n FROM t").item() == 4
    assert dfm.query("SELECT count(*) AS n FROM o").item() == 2


def test_datafusion_referenced_tables_parsing():
    from iceframe.datafusion_ops import referenced_tables

    sql = 'SELECT * FROM a.b JOIN "c"."d" ON 1=1 LEFT JOIN e USING (id) JOIN a.b ON 1=1'
    assert referenced_tables(sql) == ["a.b", "c.d", "e"]


# --------------------------------------------------------------------------
# Ray
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ray_executor():
    pytest.importorskip("ray")
    from iceframe.distributed import RayExecutor

    executor = RayExecutor(num_cpus=2, include_dashboard=False, log_to_driver=False)
    yield executor
    executor.shutdown()


def test_ray_map(ray_executor):
    assert ray_executor.map(lambda x, k: x * k, [1, 2, 3], k=2) == [2, 4, 6]


def test_ray_read_tables_parallel(ray_executor, ice, config):
    frames = ray_executor.read_tables_parallel(config, ["default.t", "sales.orders"])
    assert {name: df.height for name, df in frames.items()} == {
        "default.t": 4,
        "sales.orders": 2,
    }


# --------------------------------------------------------------------------
# Altair
# --------------------------------------------------------------------------


def test_altair_charts_render_to_vega_lite(ice):
    pytest.importorskip("altair")
    from iceframe.visualization import Visualizer

    viz = Visualizer(ice)
    charts = [
        viz.plot_distribution("default.t", "v"),
        viz.plot_scatter("default.t", "id", "v", color="name"),
        viz.plot_bar("default.t", "name", "v"),
        viz.plot_line("default.t", "id", "v"),
    ]
    for chart in charts:
        spec = chart.to_dict()
        assert spec["$schema"].startswith("https://vega.github.io/schema/vega-lite/")
        assert "default.t" in spec["title"]


# --------------------------------------------------------------------------
# Ingestion formats
# --------------------------------------------------------------------------


def test_read_delta_with_time_travel(tmp_path, rows):
    deltalake = pytest.importorskip("deltalake")
    path = str(tmp_path / "delta")
    deltalake.write_deltalake(path, rows.to_arrow())
    deltalake.write_deltalake(path, rows.to_arrow(), mode="append")
    assert ingest.read_delta(path).height == 8
    assert ingest.read_delta(path, version=0).height == 4


def test_read_lance(tmp_path, rows):
    lance = pytest.importorskip("lance")
    path = str(tmp_path / "data.lance")
    lance.write_dataset(rows.to_arrow(), path)
    assert ingest.read_lance(path).sort("id").equals(rows)


def test_read_vortex(tmp_path, rows):
    vortex = pytest.importorskip("vortex")
    path = str(tmp_path / "data.vortex")
    vortex.io.write(rows.to_arrow(), path)
    assert ingest.read_vortex(path).equals(rows)


def test_read_excel(tmp_path, rows):
    pytest.importorskip("fastexcel")
    pytest.importorskip("xlsxwriter")
    path = str(tmp_path / "data.xlsx")
    rows.write_excel(path, worksheet="Sheet1")
    assert ingest.read_excel(path).equals(rows)


def test_read_sql_via_connectorx(tmp_path, rows):
    pytest.importorskip("connectorx")
    pytest.importorskip("pandas")
    path = tmp_path / "src.db"
    with sqlite3.connect(path) as conn:
        rows.to_pandas().to_sql("src", conn, index=False)
    out = ingest.read_sql("SELECT * FROM src ORDER BY id", f"sqlite://{path}")
    assert out["id"].to_list() == [1, 2, 3, 4]


def test_read_xml(tmp_path, rows):
    pytest.importorskip("lxml")
    pytest.importorskip("pandas")
    path = str(tmp_path / "data.xml")
    rows.to_pandas().to_xml(path, index=False)
    assert ingest.read_xml(path)["name"].to_list() == ["a", "b", "c", "d"]


def test_read_stata_and_spss(tmp_path, rows):
    pyreadstat = pytest.importorskip("pyreadstat")
    pytest.importorskip("pandas")
    dta, sav = str(tmp_path / "data.dta"), str(tmp_path / "data.sav")
    rows.to_pandas().to_stata(dta, write_index=False)
    pyreadstat.write_sav(rows.to_pandas(), sav)
    assert ingest.read_stata(dta).height == 4
    assert ingest.read_spss(sav).height == 4


def test_insert_from_delta_into_iceberg(tmp_path, ice, rows):
    deltalake = pytest.importorskip("deltalake")
    path = str(tmp_path / "delta")
    deltalake.write_deltalake(path, rows.to_arrow())
    ice.create_table("default.from_delta", rows)
    ice.insert_from_file("default.from_delta", path, format="delta")
    assert ice.read_table("default.from_delta").sort("id").equals(rows)


# --------------------------------------------------------------------------
# Cache, Pydantic, federation
# --------------------------------------------------------------------------


def test_disk_cache_round_trip_and_invalidation(tmp_path, rows):
    pytest.importorskip("diskcache")
    from iceframe.cache import DiskCache

    cache = DiskCache(cache_dir=str(tmp_path / "cache"))
    cache.put("default.t", {"q": 1}, rows)
    cached = cache.get("default.t", {"q": 1})
    assert cached is not None and cached.equals(rows)
    cache.invalidate("default.t")
    assert cache.get("default.t", {"q": 1}) is None


def test_pydantic_model_to_iceberg_table(ice):
    pytest.importorskip("pydantic")
    from datetime import date, datetime

    from iceframe.pydantic import PydanticMixin, to_iceberg_schema

    class Event(PydanticMixin):
        id: int
        name: str
        score: float | None = None
        ts: datetime
        day: date
        tags: list[str] = []

    ice.create_table("default.events", to_iceberg_schema(Event))
    record = Event(id=1, name="x", ts=datetime(2026, 1, 1), day=date(2026, 1, 1), tags=["a"])
    ice.append_to_table("default.events", pl.DataFrame([record.to_iceberg_record()]))
    out = ice.read_table("default.events")
    assert out["id"].to_list() == [1]
    assert out["tags"].to_list() == [["a"]]


def test_pydantic_documented_create_and_insert_items(ice):
    """The path docs/pydantic.md shows: create_table(schema=Model) then insert_items."""
    pydantic = pytest.importorskip("pydantic")
    from datetime import datetime

    class User(pydantic.BaseModel):
        id: int
        name: str
        email: str | None = None
        created_at: datetime = datetime(2026, 1, 1)
        is_active: bool = True
        tags: list[str] = []

    ice.create_table("default.users", schema=User)
    ice.insert_items(
        "default.users",
        [
            User(id=1, name="Alice", email="a@x", tags=["x"]),
            User(id=2, name="Bob", is_active=False),
        ],
    )
    out = ice.read_table("default.users").sort("id")
    assert out["email"].to_list() == ["a@x", None]
    assert out["tags"].to_list() == [["x"], []]


def test_federation_unions_tables_across_catalogs(tmp_path, ice, config, rows):
    from iceframe.federation import CatalogFederation

    other = make_local_catalog_config(str(tmp_path / "other"))
    second = IceFrame(other)
    second.create_namespace("default")
    second.create_table("default.t", rows)
    second.append_to_table("default.t", rows)

    fed = CatalogFederation()
    fed.add_catalog("a", config)
    fed.add_catalog("b", other)
    assert fed.union_tables([("a", "default.t"), ("b", "default.t")]).height == 8


# --------------------------------------------------------------------------
# Kafka (needs a broker)
# --------------------------------------------------------------------------


@pytest.fixture
def kafka_bootstrap():
    pytest.importorskip("kafka")
    bootstrap = os.environ.get("ICEFRAME_TEST_KAFKA_BOOTSTRAP")
    if not bootstrap:
        pytest.skip("set ICEFRAME_TEST_KAFKA_BOOTSTRAP to run Kafka tests")
    return bootstrap


def _produce(bootstrap, topic, records):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode()
    )
    for record in records:
        producer.send(topic, record)
    producer.flush()
    producer.close()


def _kafka_config(bootstrap, group):
    return {
        "bootstrap_servers": bootstrap,
        "group_id": group,
        "auto_offset_reset": "earliest",
    }


def test_kafka_stream_writes_all_records_and_commits(kafka_bootstrap, ice, rows):
    from iceframe.streaming import stream_from_kafka

    topic, group = f"t-{uuid.uuid4().hex[:8]}", f"g-{uuid.uuid4().hex[:8]}"
    _produce(kafka_bootstrap, topic, [{"id": i, "name": "k", "v": float(i)} for i in range(25)])
    ice.create_table("default.stream", rows)

    written = stream_from_kafka(
        ice,
        topic,
        "default.stream",
        _kafka_config(kafka_bootstrap, group),
        batch_size=10,
        idle_timeout_seconds=5,
    )
    assert written == 25
    assert ice.read_table("default.stream").height == 25

    # Offsets were committed: a second run of the same group reads nothing new.
    again = stream_from_kafka(
        ice,
        topic,
        "default.stream",
        _kafka_config(kafka_bootstrap, group),
        idle_timeout_seconds=3,
    )
    assert again == 0
    assert ice.read_table("default.stream").height == 25


def test_kafka_stream_does_not_commit_unwritten_records(kafka_bootstrap, ice, rows):
    """A failed append must leave its records uncommitted so they are replayed."""
    from iceframe.streaming import stream_from_kafka

    topic, group = f"t-{uuid.uuid4().hex[:8]}", f"g-{uuid.uuid4().hex[:8]}"
    _produce(kafka_bootstrap, topic, [{"id": i, "name": "k", "v": float(i)} for i in range(6)])
    ice.create_table("default.replay", rows)

    real_append = ice.append_to_table
    calls = {"n": 0}

    def flaky_append(table_name, df, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated catalog outage")
        return real_append(table_name, df, **kwargs)

    ice.append_to_table = flaky_append
    with pytest.raises(RuntimeError, match="simulated"):
        stream_from_kafka(
            ice,
            topic,
            "default.replay",
            _kafka_config(kafka_bootstrap, group),
            batch_size=3,
            idle_timeout_seconds=5,
        )
    ice.append_to_table = real_append
    assert ice.read_table("default.replay").height == 3

    # The second batch was never committed, so the same group gets it again.
    replayed = stream_from_kafka(
        ice,
        topic,
        "default.replay",
        _kafka_config(kafka_bootstrap, group),
        batch_size=3,
        idle_timeout_seconds=5,
    )
    assert replayed == 3
    assert sorted(ice.read_table("default.replay")["id"].to_list()) == list(range(6))


def test_kafka_stream_flushes_on_interval_when_topic_is_quiet(kafka_bootstrap, ice, rows):
    from iceframe.streaming import stream_from_kafka

    topic, group = f"t-{uuid.uuid4().hex[:8]}", f"g-{uuid.uuid4().hex[:8]}"
    _produce(kafka_bootstrap, topic, [{"id": 1, "name": "k", "v": 1.0}])
    ice.create_table("default.quiet", rows)

    appended_at = []
    real_append = ice.append_to_table

    def recording_append(table_name, df, **kwargs):
        appended_at.append(time.time())
        return real_append(table_name, df, **kwargs)

    ice.append_to_table = recording_append
    started = time.time()
    stream_from_kafka(
        ice,
        topic,
        "default.quiet",
        _kafka_config(kafka_bootstrap, group),
        batch_size=1000,
        flush_interval_seconds=2,
        idle_timeout_seconds=8,
    )
    # One record never fills the batch; the interval flush writes it well
    # before the idle timeout ends the stream.
    assert appended_at and appended_at[0] - started < 7


def test_kafka_max_records_stops_without_committing_extra(kafka_bootstrap, ice, rows):
    from iceframe.streaming import stream_from_kafka

    topic, group = f"t-{uuid.uuid4().hex[:8]}", f"g-{uuid.uuid4().hex[:8]}"
    _produce(kafka_bootstrap, topic, [{"id": i, "name": "k", "v": float(i)} for i in range(10)])
    ice.create_table("default.capped", rows)
    cfg = _kafka_config(kafka_bootstrap, group)

    assert stream_from_kafka(ice, topic, "default.capped", cfg, max_records=4) == 4
    rest = stream_from_kafka(ice, topic, "default.capped", cfg, idle_timeout_seconds=5)
    assert rest == 6
    assert sorted(ice.read_table("default.capped")["id"].to_list()) == list(range(10))


def test_arrow_types_survive_datafusion(ice):
    pytest.importorskip("datafusion")
    out = ice.query_datafusion("SELECT id, name, v FROM default.t ORDER BY id")
    assert out.schema == pl.Schema({"id": pl.Int64, "name": pl.String, "v": pl.Float64})
    assert pa.Table.from_pylist(out.to_dicts()).num_rows == 4
