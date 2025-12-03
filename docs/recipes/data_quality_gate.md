# Data Quality Gate Recipe

This recipe demonstrates how to implement a "Write-Audit-Publish" pattern using IceFrame's branching and data quality features.

## Scenario

You want to load data into a production table, but only if it passes strict quality checks (e.g., no nulls in critical columns, values within range). If checks fail, the data should be isolated for review without affecting production.

## Implementation

```python
from iceframe import IceFrame
from iceframe.branching import BranchManager

ice = IceFrame(config)
table_name = "finance.transactions"
staging_branch = "staging_audit"

def load_with_quality_gate(new_data_df):
    # 1. Create a Branch for Staging
    # We write to a branch first so production readers don't see incomplete/bad data
    branch_manager = BranchManager(ice.catalog)
    
    # Create or replace branch pointing to current main
    try:
        branch_manager.create_branch(table_name, staging_branch)
    except:
        # If exists, fast-forward or reset (simplified here)
        pass
        
    print(f"Writing data to branch '{staging_branch}'...")
    
    # 2. Write to the Branch
    # (IceFrame write methods support a 'branch' argument if implemented, 
    # or we configure the write to target the branch ref)
    # For this recipe, we assume we can write to the branch:
    ice.append_to_table(table_name, new_data_df, branch=staging_branch)
    
    # 3. Run Data Quality Checks on the Branch
    print("Running quality checks...")
    
    # Define constraints
    constraints = [
        {"type": "not_null", "columns": ["transaction_id", "amount"]},
        {"type": "range", "column": "amount", "min": 0},
        {"type": "unique", "column": "transaction_id"}
    ]
    
    # Validate data in the branch
    # (We read from the branch to validate)
    validation_results = ice.validate_data(
        table_name, 
        constraints, 
        branch=staging_branch
    )
    
    if validation_results["passed"]:
        print("✅ Quality checks passed. Promoting to main.")
        
        # 4. Fast-Forward 'main' to 'staging_branch'
        # This makes the data visible to production users atomically
        branch_manager.fast_forward(table_name, "main", staging_branch)
        
    else:
        print("❌ Quality checks FAILED.")
        print(f"Violations: {validation_results['violations']}")
        print(f"Bad data is isolated in branch '{staging_branch}' for review.")
        # Do NOT merge to main. Alert the team.

# Example Run
df = pl.read_parquet("incoming_transactions.parquet")
load_with_quality_gate(df)
```

## Key Features Used

- **Branching**: Isolates unverified data from production readers.
- **Data Validator**: Automates quality checks.
- **Atomic Promotion**: Fast-forward merge ensures all-or-nothing visibility.
