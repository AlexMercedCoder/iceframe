"""
Pydantic integration for IceFrame.

Provides utilities to convert Pydantic models to Iceberg schemas and records.
"""

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from types import UnionType
from typing import Any, Union, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IcebergType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimeType,
)


class _FieldIds:
    """Hands out unique field ids across a schema, including nested fields."""

    def __init__(self) -> None:
        self._next = 0

    def next(self) -> int:
        self._next += 1
        return self._next


def to_iceberg_schema(model: type[BaseModel]) -> Schema:
    """
    Convert a Pydantic model to a PyIceberg Schema.

    A field is ``required`` when its type does not admit ``None``, whether or
    not it has a default: ``tags: list[str] = []`` always produces a value,
    while ``email: str | None`` can produce a null even without a default.

    Args:
        model: Pydantic model class

    Returns:
        PyIceberg Schema
    """
    return Schema(*_model_fields(model, _FieldIds()))


def _model_fields(model: type[BaseModel], ids: _FieldIds) -> list[NestedField]:
    fields = []
    for name, field_info in model.model_fields.items():
        field_id = ids.next()
        inner, nullable = _unwrap_optional(field_info.annotation)
        fields.append(
            NestedField(
                field_id=field_id,
                name=name,
                field_type=_python_type_to_iceberg(inner, ids),
                required=not nullable,
            )
        )
    return fields


def _unwrap_optional(py_type: Any) -> tuple[Any, bool]:
    """Split ``T | None`` / ``Optional[T]`` into ``(T, True)``."""
    if get_origin(py_type) in (Union, UnionType):
        args = get_args(py_type)
        non_none = [a for a in args if a is not type(None)]
        nullable = len(non_none) < len(args)
        if len(non_none) == 1:
            return non_none[0], nullable
        return py_type, nullable
    return py_type, False


def _python_type_to_iceberg(py_type: Any, ids: _FieldIds | None = None) -> IcebergType:
    """Convert Python type to Iceberg type"""
    ids = ids or _FieldIds()
    py_type, _ = _unwrap_optional(py_type)
    origin = get_origin(py_type)
    args = get_args(py_type)

    if origin in (Union, UnionType):
        # A union of several concrete types has no single Iceberg type.
        return StringType()

    # bool before int: bool is a subclass of int.
    if py_type is bool:
        return BooleanType()
    if py_type is int:
        return LongType()
    if py_type is float:
        return DoubleType()
    if py_type is str:
        return StringType()
    if py_type is bytes:
        return BinaryType()
    if py_type is Decimal:
        return DecimalType(38, 9)
    if py_type is datetime:
        return TimestampType()
    if py_type is date:
        return DateType()
    if py_type is time:
        return TimeType()
    if py_type is UUID:
        return StringType()

    if origin in (list, set, tuple) or py_type in (list, set, tuple):
        element, element_nullable = _unwrap_optional(args[0] if args else str)
        element_id = ids.next()
        return ListType(
            element_id=element_id,
            element=_python_type_to_iceberg(element, ids),
            element_required=not element_nullable,
        )

    if origin is dict or py_type is dict:
        key_type = args[0] if len(args) == 2 else str
        value, value_nullable = _unwrap_optional(args[1] if len(args) == 2 else str)
        key_id, value_id = ids.next(), ids.next()
        return MapType(
            key_id=key_id,
            key_type=_python_type_to_iceberg(key_type, ids),
            value_id=value_id,
            value_type=_python_type_to_iceberg(value, ids),
            value_required=not value_nullable,
        )

    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        return StructType(*_model_fields(py_type, ids))

    # Enums, Literals and anything else are stored as their string form.
    return StringType()


class PydanticMixin(BaseModel):
    """Mixin for Pydantic models to add Iceberg functionality"""

    def to_iceberg_record(self) -> dict[str, Any]:
        """Convert model instance to dictionary suitable for Iceberg insertion"""
        return {k: _to_iceberg_value(v) for k, v in self.model_dump().items()}


def _to_iceberg_value(value: Any) -> Any:
    """Match the schema from to_iceberg_schema: UUIDs, enums and sets as stored."""
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, dict):
        return {k: _to_iceberg_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_iceberg_value(v) for v in value]
    return value
