# ETL Pipeline Recipe

This recipe demonstrates how to build a simple ETL (Extract, Transform, Load) pipeline using IceFrame.

## Scenario

You need to ingest raw JSON logs, clean the data, enrich it, and load it into an Iceberg table partitioned by date.

## Implementation

```python
from iceframe import IceFrame
import polars as pl
from datetime import datetime

# 1. Initialize IceFrame
config = {
    "uri": "http://localhost:8181",
    "type": "rest",
    "warehouse": "s3://warehouse"
}
ice = IceFrame(config)

def run_etl_job(source_file: str, target_table: str):
    print(f"Starting ETL job for {source_file}...")

    # --- EXTRACT ---
    # Read raw data using Polars
    raw_df = pl.read_json(source_file)
    
    # --- TRANSFORM ---
    # Clean and enrich data
    processed_df = (
        raw_df
        .with_columns([
            # Convert timestamp string to datetime
            pl.col("timestamp").str.to_datetime(),
            # Extract date for partitioning
            pl.col("timestamp").str.to_datetime().dt.date().alias("event_date"),
            # Clean strings
            pl.col("user_agent").str.strip_chars(),
            # Add processing metadata
            pl.lit(datetime.now()).alias("processed_at")
        ])
        .filter(
            # Remove invalid records
            pl.col("user_id").is_not_null()
        )
    )
    
    # --- LOAD ---
    # Check if table exists, create if not
    if not ice.table_exists(target_table):
        print(f"Creating table {target_table}...")
        ice.create_table(
            target_table,
            schema=processed_df.schema,
            partition_spec=[("event_date", "identity")]
        )
    
    # Append data to Iceberg table
    print(f"Writing {processed_df.height} records to {target_table}...")
    ice.append_to_table(target_table, processed_df)
    
    print("ETL job completed successfully!")

# Run the job
run_etl_job("raw_logs.json", "analytics.web_logs")
```

## Key Features Used

- **Polars Integration**: Uses Polars for efficient in-memory transformation.
- **Schema Inference**: Automatically infers Iceberg schema from the DataFrame.
- **Partitioning**: Creates a partitioned table for optimized querying.
- **Idempotency**: Checks for table existence before creation.
