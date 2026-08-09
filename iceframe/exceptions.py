"""
IceFrame exception hierarchy.

Every error raised deliberately by IceFrame derives from :class:`IceFrameError`,
so callers can distinguish "your input was wrong" (:class:`ValidationError`,
:class:`SchemaError`) from "the infrastructure is unhappy"
(:class:`CatalogError`) without catching bare ``Exception``.

For backwards compatibility the input-shaped errors also derive from the
built-in exception types IceFrame used to raise, so existing
``except ValueError:`` handlers keep working:

* :class:`ValidationError`, :class:`SchemaError` and :class:`TableNotFoundError`
  are ``ValueError`` subclasses.
* :class:`UnsupportedOperationError` is a ``NotImplementedError`` subclass.
* :class:`CompactionError` and :class:`CatalogError` are ``RuntimeError``
  subclasses.
"""

__all__ = [
    "IceFrameError",
    "CatalogError",
    "TableNotFoundError",
    "SchemaError",
    "ValidationError",
    "CompactionError",
    "MaintenanceError",
    "UnsupportedOperationError",
]


class IceFrameError(Exception):
    """Base class for every error IceFrame raises on purpose."""


class CatalogError(IceFrameError, RuntimeError):
    """The catalog could not be reached, authenticated against, or updated."""


class TableNotFoundError(IceFrameError, ValueError):
    """The requested table does not exist in the catalog."""


class SchemaError(IceFrameError, ValueError):
    """A schema could not be built, converted, or evolved as requested."""


class ValidationError(IceFrameError, ValueError):
    """Data failed a quality/validation gate, or user input was invalid."""


class CompactionError(IceFrameError, RuntimeError):
    """A compaction / file-rewrite operation failed."""


class MaintenanceError(IceFrameError, RuntimeError):
    """A maintenance operation (expiry, orphan cleanup, manifests) failed."""


class UnsupportedOperationError(IceFrameError, NotImplementedError):
    """The operation isn't supported by the installed backend."""
