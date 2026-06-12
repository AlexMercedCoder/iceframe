# Manual verification scripts

These are ad-hoc one-off scripts that were originally committed under `tests/`
but are not pytest tests — they construct their own local SQLite-backed
warehouses in the repo root and run end-to-end scenarios that take seconds
to minutes to complete and produce on-disk side effects.

They are kept here for reference and reproduction of historical behaviour
checks. Run them directly with `python scripts/manual_verification/<name>.py`.

If a regression you find in one of these is worth catching automatically,
port it into a proper `tests/test_*.py` using `tmp_path` fixtures so it
cleans up after itself.
