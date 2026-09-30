"""Offline IceFrame performance smoke benchmark."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

import polars as pl
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from iceframe import IceFrame


def timed(fn, repeats: int) -> dict[str, float]:
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return {"median_seconds": statistics.median(samples), "min_seconds": min(samples)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.rows < 1 or args.repeats < 1:
        parser.error("--rows and --repeats must be positive")

    with tempfile.TemporaryDirectory(prefix="iceframe-bench-") as root:
        root_path = Path(root)
        ice = IceFrame(
            {
                "type": "sql",
                "uri": f"sqlite:///{root_path / 'catalog.db'}",
                "warehouse": f"file://{root_path / 'warehouse'}",
            }
        )
        ice.create_namespace("bench")
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=StringType(), required=False),
        )
        ice.create_table("bench.events", schema)
        frame = pl.DataFrame({"id": range(args.rows), "value": ["value"] * args.rows})

        results = {"rows": args.rows, "repeats": args.repeats}
        results["append"] = timed(lambda: ice.append_to_table("bench.events", frame), 1)
        results["full_scan"] = timed(lambda: ice.read_table("bench.events"), args.repeats)
        results["projected_scan"] = timed(
            lambda: ice.read_table("bench.events", columns=["id"]), args.repeats
        )
        results["filtered_scan"] = timed(
            lambda: ice.read_table("bench.events", filter_sql="id >= 0"), args.repeats
        )
        print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
