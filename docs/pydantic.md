# Pydantic Integration

IceFrame supports integration with [Pydantic](https://docs.pydantic.dev/) (v2) for schema definition and data validation.

## Installation

To use Pydantic features, install IceFrame with the `pydantic` extra:

```bash
pip install "iceframe[pydantic]"
```

## Defining Tables with Pydantic Models

You can use Pydantic models to define your table schema instead of PyArrow or PyIceberg schemas.

```python
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from iceframe import IceFrame

# Define your model
class User(BaseModel):
    id: int
    name: str
    email: Optional[str] = None
    created_at: datetime = datetime.now()
    is_active: bool = True

# Initialize IceFrame
ice = IceFrame(config)

# Create table using the model
ice.create_table("my_namespace.users", schema=User)
```

## Inserting Data

You can insert a list of Pydantic model instances directly into a table using `insert_items`.

```python
# Create user instances
users = [
    User(id=1, name="Alice", email="alice@example.com"),
    User(id=2, name="Bob", is_active=False)
]

# Insert into table
ice.insert_items("my_namespace.users", users)
```

## Type Mapping

IceFrame maps Python/Pydantic types to Iceberg types as follows:

| Python Type | Iceberg Type |
| :--- | :--- |
| `str` | `string` |
| `int` | `long` |
| `float` | `double` |
| `bool` | `boolean` |
| `datetime` | `timestamp` |
| `date` | `date` |
| `List[T]` | `list<T>` |
| `Optional[T]` | `T` (nullable) |

## Advanced Usage

For more complex schemas (nested structs, maps), you can nest Pydantic models.

```python
class Address(BaseModel):
    street: str
    city: str
    zip: str

class UserWithAddress(BaseModel):
    id: int
    name: str
    address: Address  # Maps to Iceberg Struct
```
