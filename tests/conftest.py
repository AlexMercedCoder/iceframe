"""
Pytest fixtures for IceFrame tests.

The whole core suite runs **offline** against a local SQLite catalog with a
``file://`` warehouse (PyIceberg's ``sql`` catalog). Before 0.13.0 every
read/write/query-builder/schema/stats test required a live Dremio Cloud REST
catalog and the author's credentials, so 61 tests skipped on every machine but
one — including in release validation.

Selecting a backend:

* default — local SQLite catalog, no network, no credentials.
* ``--live`` — use the REST catalog configured in ``.env``. Tests that need a
  live catalog and don't have one are skipped with a clear reason.

Fixtures:

* ``ice_frame`` — session-scoped IceFrame against the selected backend.
* ``local_ice_frame`` — always local, regardless of ``--live``.
* ``live_ice_frame`` — always the configured REST catalog; skips if unreachable.
"""

import os
import shutil
import tempfile
import time

import polars as pl
import pyarrow as pa
import pytest

from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env


def pytest_addoption(parser):
    parser.addoption(
        "--live",
        action="store_true",
        default=False,
        help="Run tests against the live REST catalog configured in .env "
        "instead of the local SQLite catalog.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "live: test requires a live REST catalog (opt in with --live)"
    )
    config.addinivalue_line("markers", "slow: long-running test")


def pytest_collection_modifyitems(config, items):
    """Skip ``@pytest.mark.live`` tests unless ``--live`` was passed."""
    if config.getoption("--live"):
        return
    skip_live = pytest.mark.skip(reason="needs a live REST catalog; run with --live")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)


def make_local_catalog_config(root: str) -> dict:
    """Build a PyIceberg ``sql`` catalog config rooted at ``root``."""
    warehouse = os.path.join(root, "warehouse")
    os.makedirs(warehouse, exist_ok=True)
    return {
        "uri": f"sqlite:///{os.path.join(root, 'catalog.db')}",
        "type": "sql",
        "warehouse": f"file://{warehouse}",
    }


@pytest.fixture(scope="session")
def local_catalog_root():
    """A temp directory holding the session's SQLite catalog + warehouse."""
    root = tempfile.mkdtemp(prefix="iceframe-tests-")
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture(scope="session")
def local_catalog_config(local_catalog_root):
    return make_local_catalog_config(local_catalog_root)


@pytest.fixture(scope="session")
def local_ice_frame(local_catalog_config):
    """An IceFrame on the local SQLite catalog. Always available, no network."""
    ice = IceFrame(local_catalog_config)
    ice.create_namespace("default")
    return ice


@pytest.fixture(scope="session")
def catalog_config(request, local_catalog_config):
    """
    Catalog configuration for the selected backend.

    Local SQLite by default; the ``.env`` REST config when ``--live`` is passed.
    """
    if request.config.getoption("--live"):
        return load_catalog_config_from_env()
    return local_catalog_config


@pytest.fixture(scope="session")
def ice_frame(request, catalog_config):
    """
    The IceFrame under test.

    Local by default. Under ``--live`` this hits the configured REST catalog
    and skips cleanly (rather than erroring on every test) when that catalog is
    unreachable.
    """
    try:
        ice = IceFrame(catalog_config)
        # Smoke check: list_namespaces forces a real call to the catalog.
        ice.list_namespaces()
    except Exception as e:
        pytest.skip(f"Catalog at {catalog_config.get('uri')!r} is unreachable: {e}")

    if not request.config.getoption("--live"):
        try:
            ice.create_namespace("default")
        except Exception:
            pass  # already exists

    return ice


@pytest.fixture(scope="session")
def live_ice_frame():
    """An IceFrame on the configured REST catalog; skips if unreachable."""
    config = load_catalog_config_from_env()
    try:
        ice = IceFrame(config)
        ice.list_namespaces()
    except Exception as e:
        pytest.skip(f"Live catalog at {config.get('uri')!r} is unreachable: {e}")
    return ice


@pytest.fixture
def fresh_ice(tmp_path):
    """
    A brand-new IceFrame with its own catalog, isolated per test.

    Use this when a test mutates catalog-wide state (drops namespaces, expires
    snapshots, removes orphan files) and must not disturb the session catalog.
    """
    ice = IceFrame(make_local_catalog_config(str(tmp_path)))
    ice.create_namespace("default")
    return ice


@pytest.fixture
def test_table_name():
    """Generate a unique test table name."""
    return f"default.test_table_{int(time.time() * 1_000_000)}"


@pytest.fixture
def sample_schema():
    """Sample PyArrow schema for testing."""
    return pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("age", pa.int32()),
        pa.field("created_at", pa.timestamp("us")),
    ])


@pytest.fixture
def sample_data():
    """Sample data matching ``sample_schema``."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": pl.Series([25, 30, 35, 40, 45], dtype=pl.Int32),
        "created_at": pl.datetime_range(
            start=pl.datetime(2024, 1, 1),
            end=pl.datetime(2024, 1, 5),
            interval="1d",
            eager=True,
        ),
    })


@pytest.fixture
def cleanup_table(ice_frame):
    """Register tables for cleanup after the test."""
    tables_to_cleanup = []

    def register_table(table_name):
        tables_to_cleanup.append(table_name)

    yield register_table

    for table_name in tables_to_cleanup:
        try:
            if ice_frame.table_exists(table_name):
                ice_frame.drop_table(table_name)
        except Exception as e:  # pragma: no cover - cleanup is best-effort
            print(f"Failed to cleanup table {table_name}: {e}")
