# Contributing to IceFrame

IceFrame welcomes focused bug fixes, tests, documentation improvements, and
well-validated features. Open an issue before starting a large API addition so
the compatibility and maintenance cost can be discussed first.

## Development setup

```bash
python -m pip install -e ".[dev]"
ruff format iceframe tests benchmarks
ruff check iceframe tests benchmarks
mypy
pytest --cov=iceframe --cov-fail-under=65
python -m build
python -m twine check dist/*
```

The mypy command checks the complete `iceframe` package, including bodies of
functions that do not yet require annotations, and must remain at zero errors.
The default tests use a temporary SQLite catalog and require no credentials.
Use `pytest --live` only when intentionally validating a configured REST
catalog. Never commit `.env`, credentials, warehouse data, or generated build
artifacts.

Every bug fix should include a regression test. Features that depend on an
optional package need a real contract test against a supported version rather
than a mock that invents the dependency's API.

Put those contract tests in `tests/test_integrations_real.py`, guarded with
`pytest.importorskip`. Catalog-dependent behavior belongs in
`tests/compat_checks.py`, which runs against every catalog in CI and feeds
`docs/compatibility.md` (regenerate it with `python scripts/compat_matrix.py`).
Never assign fakes into `sys.modules` at module level: they leak into every
later test in the session. Use `patch.dict(sys.modules, ...)` scoped to the
test or module.

## Compatibility

Public API removals require a deprecation period and a changelog entry. Core
support follows the Python and PyIceberg versions declared in `pyproject.toml`.
Experimental integrations are identified in the documentation and may change
between minor releases while IceFrame remains alpha.
