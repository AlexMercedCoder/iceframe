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
| `bytes` | `binary` |
| `Decimal` | `decimal(38, 9)` |
| `datetime` | `timestamp` |
| `date` | `date` |
| `time` | `time` |
| `UUID`, `Enum`, `Literal` | `string` |
| `list[T]`, `set[T]`, `tuple[T, ...]` | `list<T>` |
| `dict[K, V]` | `map<K, V>` |
| A nested `BaseModel` | `struct` |
| `T \| None`, `Optional[T]` | `T`, optional |

A field is **required** when its type does not allow `None`, whether or not it
has a default. `tags: list[str] = []` always has a value, so it is required;
`email: str | None` can be null even without a default, so it is optional.
Every field gets a unique id, including fields inside nested models, lists
and maps.

Polars marks every column nullable, so IceFrame accepts a DataFrame for a
table with required fields as long as those columns contain no nulls, at any
nesting level. A real null in a required column is still rejected.

Before 0.15.0, `X | None` fell through to `string` (only `Optional[X]` was
recognized), required was based on the default rather than the type, and
nested field ids collided.

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
