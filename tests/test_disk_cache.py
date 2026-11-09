from pathlib import Path

import polars as pl
import pytest

diskcache_community = pytest.importorskip("diskcache_community")

from iceframe.cache import DiskCache

PARAMS = {"q": 1}


def _write_marker(path: str) -> None:
    Path(path).write_text("ran")


class _PlantedEntry:
    def __init__(self, marker: Path):
        self.marker = str(marker)

    def __reduce__(self):
        return (_write_marker, (self.marker,))


def test_disk_cache_planted_pickle_is_a_miss(tmp_path):
    cache_dir = tmp_path / "cache"
    marker = tmp_path / "planted-code-ran"
    df = pl.DataFrame({"id": [2, 3], "g": ["a", "b"]})
    cache = DiskCache(cache_dir=str(cache_dir))
    cache.put("default.t", PARAMS, df, ttl=300)

    with diskcache_community.Cache(
        str(cache_dir / "index"), disk=diskcache_community.Disk
    ) as writer:
        (key,) = list(writer)
        writer.set(key, _PlantedEntry(marker))

    assert cache.get("default.t", PARAMS) is None
    assert not marker.exists()
