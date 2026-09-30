# Performance benchmarks

`benchmark_local.py` is a repeatable, offline smoke benchmark for append,
full-scan, projected-scan, and filtered-scan paths. It uses a temporary SQLite
catalog and warehouse, prints machine-readable JSON, and leaves no data behind.

```bash
python -m benchmarks.benchmark_local --rows 100000 --repeats 3
```

Record the JSON before and after performance-sensitive changes. Investigate a
median regression over 15% on the same machine; absolute timings are not
comparable across machines.
