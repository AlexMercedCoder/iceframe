"""
DataFusion unit tests.

Behaviour against the real library lives in tests/test_integrations_real.py.
These tests used to mock the whole datafusion module, which is how 0.14.0
shipped an integration that failed on every query with DataFusion 54.
"""

from unittest.mock import MagicMock, patch

import pytest

from iceframe.datafusion_ops import DataFusionManager, referenced_tables


def test_missing_datafusion_raises_install_hint():
    with patch("iceframe.datafusion_ops.DATAFUSION_AVAILABLE", False):
        with pytest.raises(ImportError, match=r"iceframe\[datafusion\]"):
            DataFusionManager(MagicMock())


def test_referenced_tables_handles_quotes_case_and_duplicates():
    sql = """
        select * FROM sales.orders o
        Join "analytics"."users" u on o.uid = u.id
        left join sales.orders again on 1 = 1
        where o.id in (select id from default.t)
    """
    assert referenced_tables(sql) == ["sales.orders", "analytics.users", "default.t"]
