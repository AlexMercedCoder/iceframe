"""
Run the compatibility checks against every configured catalog.

The local SQLite catalog always runs. REST catalogs run when configured:

    ICEFRAME_COMPAT_REST_URI=http://localhost:18181
    ICEFRAME_COMPAT_POLARIS_URI=http://localhost:18182/api/catalog

See ``ci/catalogs-compose.yml`` for containers that match these settings.
"""

import tempfile
import uuid

import pytest

from iceframe import IceFrame
from tests.compat_checks import CHECKS, CatalogLacksFeature, catalogs_from_env

_ROOT = tempfile.mkdtemp(prefix="iceframe-compat-")
CATALOGS = catalogs_from_env(_ROOT)


@pytest.fixture(scope="module", params=list(CATALOGS), ids=list(CATALOGS))
def catalog(request, tmp_path_factory):
    ice = IceFrame(CATALOGS[request.param])
    ns = f"compat_{uuid.uuid4().hex[:8]}"
    ice.create_namespace(ns)
    return ice, ns, str(tmp_path_factory.mktemp("compat"))


@pytest.mark.parametrize("check", list(CHECKS), ids=list(CHECKS))
def test_catalog_feature(catalog, check):
    ice, ns, tmp = catalog
    try:
        CHECKS[check](ice, ns, tmp)
    except CatalogLacksFeature as e:
        pytest.skip(f"catalog does not support this: {e}")
