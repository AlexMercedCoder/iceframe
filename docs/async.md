# Async Support

IceFrame provides async versions of core operations for non-blocking execution.

## AsyncIceFrame

```python
import asyncio
from iceframe.async_ops import AsyncIceFrame

async def main():
    config = {...}
    async_ice = AsyncIceFrame(config)
    
    # Async read
    df = await async_ice.read_table_async("users")
    
    # Async write
    await async_ice.append_to_table_async("users", new_data)
    
    # Async stats
    stats = await async_ice.stats_async("users")

asyncio.run(main())
```

## Async Query Builder

```python
from iceframe.expressions import Column

async def query_data():
    async_ice = AsyncIceFrame(config)
    
    query = await async_ice.query_async("users")
    result = await (query
        .filter(Column("age") > 25)
        .execute_async())
    
    return result

df = asyncio.run(query_data())
```

## Use Cases

- **High Concurrency**: Handle multiple table operations concurrently
- **Web Applications**: Non-blocking API endpoints
- **Data Pipelines**: Parallel processing of multiple tables
