import unittest
from typing import Optional, List
from pydantic import BaseModel
from pyiceberg.types import (
    StringType, LongType, DoubleType, BooleanType, 
    TimestampType, DateType, ListType, StructType, NestedField
)
from iceframe.pydantic import to_iceberg_schema

class TestPydanticIntegration(unittest.TestCase):
    
    def test_simple_model(self):
        class User(BaseModel):
            id: int
            name: str
            is_active: bool
            score: float
            
        schema = to_iceberg_schema(User)
        fields = {f.name: f.field_type for f in schema.fields}
        
        self.assertIsInstance(fields["id"], LongType)
        self.assertIsInstance(fields["name"], StringType)
        self.assertIsInstance(fields["is_active"], BooleanType)
        self.assertIsInstance(fields["score"], DoubleType)
        
    def test_optional_fields(self):
        class User(BaseModel):
            id: int
            email: Optional[str] = None
            
        schema = to_iceberg_schema(User)
        # Find email field
        email_field = next(f for f in schema.fields if f.name == "email")
        self.assertFalse(email_field.required)
        self.assertIsInstance(email_field.field_type, StringType)
        
        id_field = next(f for f in schema.fields if f.name == "id")
        self.assertTrue(id_field.required)

    def test_nested_model(self):
        class Address(BaseModel):
            street: str
            city: str
            
        class User(BaseModel):
            name: str
            address: Address
            
        schema = to_iceberg_schema(User)
        address_field = next(f for f in schema.fields if f.name == "address")
        
        self.assertIsInstance(address_field.field_type, StructType)
        struct_fields = {f.name: f.field_type for f in address_field.field_type.fields}
        self.assertIsInstance(struct_fields["street"], StringType)
        self.assertIsInstance(struct_fields["city"], StringType)

    def test_list_type(self):
        class User(BaseModel):
            tags: List[str]
            
        schema = to_iceberg_schema(User)
        tags_field = next(f for f in schema.fields if f.name == "tags")
        
        self.assertIsInstance(tags_field.field_type, ListType)
        self.assertIsInstance(tags_field.field_type.element_type, StringType)

if __name__ == '__main__':
    unittest.main()
