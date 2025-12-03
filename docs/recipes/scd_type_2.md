# Slowly Changing Dimensions (SCD) Type 2 Recipe

This recipe demonstrates how to implement SCD Type 2 (retaining full history) using IceFrame's merge capabilities.

## Scenario

You have a `users` table and receive updates. You want to track historical changes to user profiles (e.g., address changes) by creating new records for updates while marking old records as inactive.

## Implementation

```python
from iceframe import IceFrame
from iceframe.expressions import Column
import polars as pl

ice = IceFrame(config)
target_table = "dim.users"

def process_scd_type_2(updates_df: pl.DataFrame):
    """
    Apply SCD Type 2 updates to the users dimension table.
    Schema: user_id, name, address, is_active, valid_from, valid_to
    """
    
    # 1. Identify records that have actually changed
    # Read current active records
    current_df = (
        ice.query(target_table)
        .filter(Column("is_active") == True)
        .execute()
    )
    
    # Join to find changes
    # (In a real scenario, you'd hash columns to detect changes efficiently)
    
    # 2. Prepare the MERGE operation
    # For SCD Type 2, we typically do this in two steps or use a complex merge.
    # Here is a simplified approach using IceFrame's merge:
    
    # Step A: Expire old records
    # Update existing records where ID matches but content differs
    (ice.query(target_table)
        .merge(updates_df, on="user_id")
        .when_matched_update({
            "is_active": False,
            "valid_to": pl.col("source.valid_from")
        })
        .execute())
        
    # Step B: Insert new versions
    # Insert new records for all updates
    new_records = updates_df.with_columns([
        pl.lit(True).alias("is_active"),
        pl.lit(None).alias("valid_to")
    ])
    
    ice.append_to_table(target_table, new_records)

# Example Usage
updates = pl.DataFrame({
    "user_id": [101, 102],
    "name": ["Alice", "Bob"],
    "address": ["New Address St", "Same Address"],
    "valid_from": [datetime.now(), datetime.now()]
})

process_scd_type_2(updates)
```

> [!NOTE]
> True SCD Type 2 often requires complex logic to handle out-of-order data and exact timestamp alignment. This recipe shows the fundamental pattern of expiring old rows and inserting new ones.

## Key Features Used

- **Merge Operation**: Used to update existing records based on a key.
- **Predicate Pushdown**: Efficiently reads only active records.
