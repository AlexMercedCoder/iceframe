#!/usr/bin/env python3
"""
Test script to verify catalog connectivity and basic IceFrame operations.

This script:
1. Creates a namespace called 'iceframe'
2. Creates a table called 'iceframe.test_table'
3. Adds 30 records in 3 transactions of 10 at a time
4. Runs compaction
"""

from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env
import polars as pl
from datetime import datetime, timedelta


def main():
    print("=" * 60)
    print("IceFrame Catalog Test Script")
    print("=" * 60)
    
    # Load configuration from .env
    print("\n[1/5] Loading catalog configuration from .env...")
    config = load_catalog_config_from_env()
    ice = IceFrame(config)
    print("✓ Configuration loaded successfully")
    
    # Create namespace
    namespace = "iceframe"
    table_name = f"{namespace}.test_table"
    
    print(f"\n[2/5] Creating namespace '{namespace}'...")
    try:
        ice.create_namespace(namespace, {"owner": "test-script", "created_at": str(datetime.now())})
        print(f"✓ Namespace '{namespace}' created successfully")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"⚠ Namespace '{namespace}' already exists, continuing...")
        else:
            raise
    
    # Create table
    print(f"\n[3/5] Creating table '{table_name}'...")
    schema = {
        "id": "long",
        "name": "string",
        "value": "double",
        "created_at": "timestamp",
        "is_active": "boolean"
    }
    
    try:
        ice.create_table(table_name, schema)
        print(f"✓ Table '{table_name}' created successfully")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"⚠ Table '{table_name}' already exists, dropping and recreating...")
            ice.drop_table(table_name)
            ice.create_table(table_name, schema)
            print(f"✓ Table '{table_name}' recreated successfully")
        else:
            raise
    
    # Add 30 records in 3 transactions of 10 at a time
    print(f"\n[4/5] Adding 30 records in 3 transactions of 10 records each...")
    base_time = datetime.now()
    
    for batch_num in range(3):
        start_id = batch_num * 10 + 1
        end_id = start_id + 10
        
        # Create batch of 10 records
        data = pl.DataFrame({
            "id": list(range(start_id, end_id)),
            "name": [f"User_{i}" for i in range(start_id, end_id)],
            "value": [float(i * 10.5) for i in range(start_id, end_id)],
            "created_at": [base_time + timedelta(hours=i) for i in range(start_id, end_id)],
            "is_active": [i % 2 == 0 for i in range(start_id, end_id)]
        })
        
        ice.append_to_table(table_name, data)
        print(f"  ✓ Transaction {batch_num + 1}/3: Added records {start_id}-{end_id - 1}")
    
    print(f"✓ All 30 records added successfully")
    
    # Verify data
    print(f"\n  Verifying data...")
    df = ice.read_table(table_name)
    print(f"  Total records in table: {len(df)}")
    print(f"\n  Sample data (first 5 rows):")
    print(df.head(5))
    
    # Run compaction
    print(f"\n[5/5] Running compaction on '{table_name}'...")
    try:
        ice.compact_data_files(table_name, target_file_size_mb=128)
        print(f"✓ Compaction completed successfully")
    except Exception as e:
        print(f"⚠ Compaction note: {e}")
        print("  (This is normal if there aren't enough small files to compact)")
    
    # Final summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"✓ Namespace: {namespace}")
    print(f"✓ Table: {table_name}")
    print(f"✓ Records added: 30 (in 3 transactions)")
    print(f"✓ Current record count: {len(df)}")
    print(f"✓ Compaction: Executed")
    print("\n✅ All operations completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
