# Distributed Processing with Ray

IceFrame integrates with Ray to scale your data processing across a cluster of machines.

## Installation

```bash
pip install "iceframe[distributed]"
```

## Usage

Access the distributed executor via the `distribute` property.

```python
# Initialize Ray (if not already running)
executor = ice.distribute

# Parallel Map
# Apply a function to a list of items in parallel
results = executor.map(
    lambda x: x * 2, 
    [1, 2, 3, 4, 5]
)

# Parallel Table Reading
# Read multiple tables concurrently
dfs = executor.read_tables_parallel(
    ice_frame_config=ice.config,
    table_names=["table1", "table2", "table3"]
)
```

## Configuration

The `RayExecutor` will automatically connect to an existing Ray cluster if available, or start a local one. You can pass arguments to `ray.init()` via the `RayExecutor` constructor if you instantiate it manually.
