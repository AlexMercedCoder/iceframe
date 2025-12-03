# Incremental Ingestion Recipe

This recipe demonstrates how to process data incrementally, reading only what has changed since the last run.

## Scenario

You have a downstream table `daily_summary` that aggregates data from an upstream `raw_events` table. You want to run this aggregation periodically, processing only new data added to `raw_events`.

## Implementation

```python
from iceframe import IceFrame
import json
import os

ice = IceFrame(config)
state_file = "ingestion_state.json"

def get_last_snapshot_id():
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return json.load(f).get("last_snapshot_id")
    return None

def save_state(snapshot_id):
    with open(state_file, "w") as f:
        json.dump({"last_snapshot_id": snapshot_id}, f)

def run_incremental_job():
    source_table = "raw_events"
    target_table = "daily_summary"
    
    # 1. Get last processed state
    last_snapshot = get_last_snapshot_id()
    
    # 2. Read incremental data
    print(f"Reading {source_table} since snapshot {last_snapshot}...")
    
    # Use IceFrame's incremental reader
    # This returns a DataFrame containing only new rows appended since snapshot
    new_data = ice.read_incremental(
        source_table, 
        since_snapshot_id=last_snapshot
    )
    
    if new_data.height == 0:
        print("No new data found.")
        return

    # 3. Process the data (Aggregation)
    summary_df = (
        new_data
        .group_by("event_date", "category")
        .agg([
            pl.count().alias("event_count"),
            pl.sum("amount").alias("total_amount")
        ])
    )
    
    # 4. Write to target (Upsert/Merge)
    # We merge into the summary table to update counts
    (ice.query(target_table)
        .merge(summary_df, on=["event_date", "category"])
        .when_matched_update({
            "event_count": pl.col("target.event_count") + pl.col("source.event_count"),
            "total_amount": pl.col("target.total_amount") + pl.col("source.total_amount")
        })
        .when_not_matched_insert({
            "event_date": pl.col("source.event_date"),
            "category": pl.col("source.category"),
            "event_count": pl.col("source.event_count"),
            "total_amount": pl.col("source.total_amount")
        })
        .execute())
        
    # 5. Update state
    # Get the current snapshot ID of the source table to save for next time
    current_snapshot = ice.get_table(source_table).current_snapshot().snapshot_id
    save_state(current_snapshot)
    print(f"Job finished. State updated to snapshot {current_snapshot}")

run_incremental_job()
```

## Key Features Used

- **Incremental Read**: `read_incremental` efficiently fetches only new data.
- **Merge/Upsert**: Updates existing aggregates or inserts new ones.
- **State Management**: Tracks progress via snapshot IDs.
