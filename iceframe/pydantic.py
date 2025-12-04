"""
Pydantic integration for IceFrame.

Provides utilities to convert Pydantic models to Iceberg schemas and records.
"""

from typing import Type, List, Dict, Any, Union, Optional, get_origin, get_args
from pydantic import BaseModel
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    TimestampType,
    DateType,
    ListType,
    StructType,
    MapType,
    IcebergType
)

def to_iceberg_schema(model: Type[BaseModel]) -> Schema:
    """
    Convert a Pydantic model to a PyIceberg Schema.
    
    Args:
        model: Pydantic model class
        
    Returns:
        PyIceberg Schema
    """
    fields = []
    for i, (name, field_info) in enumerate(model.model_fields.items()):
        # Determine if field is optional (nullable)
        # Pydantic v2 uses annotation
        annotation = field_info.annotation
        required = field_info.is_required()
        
        iceberg_type = _python_type_to_iceberg(annotation)
        
        fields.append(
            NestedField(
                field_id=i + 1,
                name=name,
                field_type=iceberg_type,
                required=required
            )
        )
        
    return Schema(*fields)

def _python_type_to_iceberg(py_type: Type) -> IcebergType:
    """Convert Python type to Iceberg type"""
    origin = get_origin(py_type)
    args = get_args(py_type)
    
    # Handle Optional/Union[T, None]
    if origin is Union:
        # Check if None is in args
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            return _python_type_to_iceberg(non_none_args[0])
        # Complex unions not supported yet, default to string?
        # Or maybe Struct?
        return StringType()
        
    if py_type is str:
        return StringType()
    elif py_type is int:
        return LongType() # Default to Long for safety
    elif py_type is float:
        return DoubleType()
    elif py_type is bool:
        return BooleanType()
    
    # Handle Lists
    if origin is list or origin is List:
        element_type = args[0] if args else str
        return ListType(
            element_id=0, # ID will be assigned by schema creation? No, need to be careful
            element=_python_type_to_iceberg(element_type),
            element_required=False # Assume elements can be null?
        )
        
    # Handle nested Pydantic models
    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        fields = []
        for i, (name, field_info) in enumerate(py_type.model_fields.items()):
            fields.append(
                NestedField(
                    field_id=i + 1, # Nested IDs need management
                    name=name,
                    field_type=_python_type_to_iceberg(field_info.annotation),
                    required=field_info.is_required()
                )
            )
        return StructType(*fields)

    # Date/Time handling requires more specific types (datetime.date, datetime.datetime)
    from datetime import date, datetime
    if py_type is datetime:
        return TimestampType()
    if py_type is date:
        return DateType()
        
    # Default fallback
    return StringType()

class PydanticMixin:
    """Mixin for Pydantic models to add Iceberg functionality"""
    
    def to_iceberg_record(self) -> Dict[str, Any]:
        """Convert model instance to dictionary suitable for Iceberg insertion"""
        return self.model_dump()
