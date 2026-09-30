# Async Support

IceFrame provides a bounded-thread async facade over PyIceberg's synchronous
APIs. It keeps an event loop responsive during catalog and scan work; it is not
native async I/O and does not make CPU-bound processing faster.

## AsyncIceFrame

```python
import asyncio
from iceframe.async_ops import AsyncIceFrame

async def main():
    config = {...}
    async with AsyncIceFrame(config, max_workers=4) as async_ice:
        df = await async_ice.read_table("users")
        await async_ice.append_to_table("users", new_data)
        stats = await async_ice.stats("users")

asyncio.run(main())
```

## Async Query Builder

```python
from iceframe.expressions import Column

async def query_data():
    async with AsyncIceFrame(config) as async_ice:
        query = await async_ice.query("users")
        return await query.filter(Column("age") > 25).execute()

df = asyncio.run(query_data())
```

## Use Cases

- **Bounded Concurrency**: Handle several I/O-bound table operations without an
  unbounded global executor
- **Web Applications**: Non-blocking API endpoints
- **Data Pipelines**: Parallel processing of multiple tables
