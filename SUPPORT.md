# Support and release policy

IceFrame is alpha software. The latest minor release is the supported line;
users should upgrade for correctness and security fixes. Python 3.10-3.13 and
the PyIceberg range declared in `pyproject.toml` are tested in CI.

Use GitHub Issues for reproducible bugs and feature proposals. Include the
IceFrame, Python, PyIceberg, Polars, catalog type, and storage backend versions,
plus a minimal reproduction with credentials removed.

Releases follow semantic versioning where practical. While the project remains
0.x, incompatible experimental-feature changes may occur in minor releases,
but established core APIs receive a deprecation period and changelog guidance.
