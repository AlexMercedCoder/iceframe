"""
Query Builder for IceFrame.

Provides a fluent API for building and executing queries on Iceberg tables.
"""

from typing import Any, List, Optional, Union, Dict
import polars as pl
from pyiceberg.table import Table
from pyiceberg.expressions import AlwaysTrue

from iceframe.expressions import Expression, Column, LiteralValue
from iceframe.operations import TableOperations


class QueryBuilder:
    """Fluent API for building queries"""
    
    def __init__(self, operations: TableOperations, table_name: str):
        self.operations = operations
        self.table_name = table_name
        self._select_exprs = []
        self._filter_exprs = []
        self._group_by_exprs = []
        self._order_by_exprs = []
        self._limit = None
        self._with_columns = []
        self._joins = []  # List of (table_name, on, how) tuples
        self._cache_ttl = None  # Cache TTL in seconds
    
    def select(self, *exprs: Union[str, Expression]) -> 'QueryBuilder':
        """Select columns or expressions"""
        for expr in exprs:
            if isinstance(expr, str):
                self._select_exprs.append(Column(expr))
            else:
                self._select_exprs.append(expr)
        return self
    
    def filter(self, expr: Expression) -> 'QueryBuilder':
        """Filter rows (WHERE clause)"""
        self._filter_exprs.append(expr)
        return self
    
    def where(self, expr: Expression) -> 'QueryBuilder':
        """Alias for filter"""
        return self.filter(expr)
    
    def join(
        self,
        other_table: str,
        on: Union[str, List[str]],
        how: str = "inner"
    ) -> 'QueryBuilder':
        """
        Join with another table.
        
        Args:
            other_table: Name of the table to join with
            on: Column name(s) to join on
            how: Join type - "inner", "left", "right", "outer"
            
        Returns:
            Self for chaining
        """
        if how not in ["inner", "left", "right", "outer"]:
            raise ValueError(f"Invalid join type: {how}. Must be one of: inner, left, right, outer")
            
        self._joins.append((other_table, on, how))
        return self
    
    def group_by(self, *exprs: Union[str, Expression]) -> 'QueryBuilder':
        """Group by columns or expressions"""
        for expr in exprs:
            if isinstance(expr, str):
                self._group_by_exprs.append(Column(expr))
            else:
                self._group_by_exprs.append(expr)
        return self
    
    def order_by(self, *exprs: Union[str, Expression]) -> 'QueryBuilder':
        """Order by columns or expressions"""
        for expr in exprs:
            if isinstance(expr, str):
                self._order_by_exprs.append(Column(expr))
            else:
                self._order_by_exprs.append(expr)
        return self
    
    def limit(self, n: int) -> 'QueryBuilder':
        """Limit number of rows"""
        self._limit = n
        return self
    
    def with_column(self, name: str, expr: Expression) -> 'QueryBuilder':
        """Add or replace a column"""
        self._with_columns.append((name, expr))
        return self
    
    def cache(self, ttl: Optional[int] = None) -> 'QueryBuilder':
        """
        Enable caching for this query.
        
        Args:
            ttl: Time to live in seconds (None = no expiration)
            
        Returns:
            Self for chaining
        """
        self._cache_ttl = ttl
        return self
    
    def execute(self) -> pl.DataFrame:
        """Execute the query and return a Polars DataFrame"""
        # 1. Predicate Pushdown
        # Identify filters that can be pushed down to PyIceberg
        iceberg_filters = []
        polars_filters = []
        
        for expr in self._filter_exprs:
            ice_expr = expr.to_iceberg()
            if not isinstance(ice_expr, AlwaysTrue):
                iceberg_filters.append(ice_expr)
            else:
                # If can't be pushed down, keep for Polars
                polars_filters.append(expr)
        
        # Combine iceberg filters
        if iceberg_filters:
            from pyiceberg.expressions import And
            combined_filter = iceberg_filters[0]
            for f in iceberg_filters[1:]:
                combined_filter = And(combined_filter, f)
        else:
            combined_filter = AlwaysTrue()
            
        # 2. Read from Iceberg
        table = self.operations.get_table(self.table_name)
        scan = table.scan(row_filter=combined_filter)
        
        # Execute scan
        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)
        
        # 3. Handle Joins
        if self._joins:
            for join_table_name, on, how in self._joins:
                # Read the join table
                join_table = self.operations.get_table(join_table_name)
                join_scan = join_table.scan()
                join_arrow = join_scan.to_arrow()
                join_df = pl.from_arrow(join_arrow)
                
                # Perform join
                df = df.join(join_df, on=on, how=how)
        
        # 4. Polars Post-processing
        
        # Apply remaining filters
        for expr in polars_filters:
            df = df.filter(expr.to_polars())
            
        # Apply with_columns
        for name, expr in self._with_columns:
            df = df.with_columns(expr.to_polars().alias(name))
            
        # Apply Group By
        if self._group_by_exprs:
            # If we have group by, select expressions must be aggregations
            group_cols = [e.to_polars() for e in self._group_by_exprs]
            
            if not self._select_exprs:
                # If no select specified, return groups? Or count?
                # Standard SQL requires select with group by
                raise ValueError("SELECT clause required with GROUP BY")
            
            # Identify grouping column names to avoid duplication in agg
            group_col_names = set()
            for expr in self._group_by_exprs:
                if isinstance(expr, Column):
                    group_col_names.add(expr.name)
                # Note: Complex expressions in group by might need more complex handling
                # for deduplication, but for now we handle simple columns.

            agg_exprs = []
            for expr in self._select_exprs:
                # If it's a simple column and in group keys, skip adding to agg
                # because Polars adds group keys automatically to the result
                if isinstance(expr, Column) and expr.name in group_col_names:
                    continue
                agg_exprs.append(expr.to_polars())
            
            df = df.group_by(group_cols).agg(agg_exprs)
        
        elif self._select_exprs:
            # No group by, just select
            select_cols = [e.to_polars() for e in self._select_exprs]
            df = df.select(select_cols)
            
        # Apply Order By
        if self._order_by_exprs:
            # Polars sort expects column names or expressions
            # If expressions are complex, we might need to select them first or use sort_by
            # For simplicity, assume simple columns or expressions valid in sort
            sort_exprs = [e.to_polars() for e in self._order_by_exprs]
            df = df.sort(sort_exprs)
            
        # Apply Limit
        if self._limit is not None:
            df = df.head(self._limit)
            
        return df

    # Write Operations
    
    def insert(self, data: Union[pl.DataFrame, Dict[str, List[Any]]]) -> None:
        """Insert data into the table"""
        self.operations.append_to_table(self.table_name, data)
        
    def delete(self) -> None:
        """Delete rows matching the filter"""
        # Construct filter expression for deletion
        # Note: Delete in PyIceberg usually takes a PyIceberg expression
        
        if not self._filter_exprs:
            raise ValueError("DELETE requires a filter (use filter/where)")
            
        # Combine filters
        from pyiceberg.expressions import And
        combined_filter = self._filter_exprs[0].to_iceberg()
        for f in self._filter_exprs[1:]:
            combined_filter = And(combined_filter, f.to_iceberg())
            
        if isinstance(combined_filter, AlwaysTrue):
             # If filters couldn't be converted to Iceberg expressions
             raise ValueError("Complex filters cannot be used for DELETE operations yet")
             
        # Execute delete
        # Note: operations.delete_from_table expects a string expression currently
        # We should update it or use the table object directly
        table = self.operations.get_table(self.table_name)
        table.delete(combined_filter)

    def update(self, updates: Dict[str, Any]) -> None:
        """
        Update rows matching the filter.
        
        Optimized Copy-on-Write:
        1. Identify affected partitions (if partitioned).
        2. Read only affected partitions.
        3. Apply updates in memory.
        4. Overwrite those partitions.
        """
        if not self._filter_exprs:
             raise ValueError("UPDATE requires a filter")
             
        table = self.operations.get_table(self.table_name)
        spec = table.spec()
        
        # Build filter mask for Polars
        mask = None
        for expr in self._filter_exprs:
            condition = expr.to_polars()
            if mask is None:
                mask = condition
            else:
                mask = mask & condition
                
        # Build Iceberg filter for identification
        from pyiceberg.expressions import And
        ice_filter = self._filter_exprs[0].to_iceberg()
        for f in self._filter_exprs[1:]:
            ice_filter = And(ice_filter, f.to_iceberg())
            
        update_exprs = []
        for col_name, new_value in updates.items():
            val_expr = pl.lit(new_value)
            update_exprs.append(
                pl.when(mask)
                .then(val_expr)
                .otherwise(pl.col(col_name))
                .alias(col_name)
            )

        # Check if table is partitioned
        if not spec.fields:
            # Not partitioned - full rewrite
            print("Table not partitioned, performing full rewrite.")
            df = self.operations.read_table(self.table_name)
            df = df.with_columns(update_exprs)
            self.operations.overwrite_table(self.table_name, df)
            return

        # Partitioned Optimization
        # 1. Identify affected data files/partitions
        # We can scan with the filter and get the list of files.
        # Then, for each unique partition tuple found, we rewrite THAT partition.
        
        # Note: Accessing partition values from scan tasks might be complex.
        # Simpler approach:
        # 1. Scan matched data to get distinct partition values.
        # This assumes we can select partition cols.
        # If partition cols are hidden (transforms), we need to select source cols and re-derive?
        # Or just select existing partition columns if they exist in schema.
        
        # Let's try to get distinct values of partition source columns from the filtered data.
        # This is accurate enough.
        
        partition_cols = [f.name for f in spec.fields]
        # Note: field names in spec might be source_column or transformed name.
        # We need source column names for querying.
        # spec.fields[i].source_id maps to schema.
        
        # Let's just scan the matching rows using the filter
        # And select * (or just needed cols)
        # Then find the distinct partitions.
        # But wait, we need to overwrite the *whole* partition, so we need to know WHICH partitions.
        
        # Strategy:
        # 1. Read matches: `df_matches = scan(filter=ice_filter).to_polars()`
        # 2. Get unique partitions: `partitions = df_matches.select(partition_source_cols).unique()`
        # 3. For each partition P:
        #    a. Read ALL data for P: `df_p = scan(filter=P).to_polars()` (ignoring the update filter)
        #    b. Apply update logic to `df_p` using the update filter
        #    c. Overwrite P using `overwrite(df_p, overwrite_filter=P)`
        
        # Problem: `overwrite` in PyIceberg might not support `overwrite_filter` easily in high-level API?
        # IceFrame's `overwrite_table` does `table.overwrite(df.to_arrow())`.
        # PyIceberg's `overwrite` will replace data that matches the input dataframe? 
        # No, `overwrite` typically replaces data matching a filter OR replaces all data.
        # PyIceberg `overwrite` accepts a `DataFile` list or similar?
        # Actually PyIceberg `overwrite` method on Table:
        # `def overwrite(self, df: pa.Table, overwrite_filter: BooleanExpression = AlwaysTrue())`
        # YES! It supports specific overwrite filter.
        
        # So we need to:
        # 1. Identify source columns for partitioning
        schema = table.schema()
        source_col_ids = [f.source_id for f in spec.fields]
        source_col_names = [schema.find_field(id).name for id in source_col_ids]
        
        # 2. Scan to find Affected Partitions
        # We only need to read the partition columns to identify them
        affected_rows = table.scan(
            row_filter=ice_filter,
            selected_fields=tuple(source_col_names)
        ).to_arrow()
        
        if len(affected_rows) == 0:
            return # No updates needed
            
        affected_df = pl.from_arrow(affected_rows)
        distinct_partitions = affected_df.unique()
        
        print(f"Updating {distinct_partitions.height} partitions...")
        
        # 3. Loop and Update
        # Ideally we batch this or do it in one go if filter can be constructed?
        # If we construct a filter "PartCol IN (val1, val2...)" we can read all those partitions at once.
        # But `overwrite` needs to know it's replacing THOSE partitions.
        # If we pass a filter to `overwrite` matching those partitions, and valid data for those partitions, it should work.
        
        # Construct a filter for ALL affected partitions
        # This might be big if many partitions.
        # Let's do it per partition for safety/memory, or grouped?
        # Let's try doing it for all affected partitions at once (if memory allows).
        # Safe Compaction will handle memory later.
        
        # Build filter: (P1_col == val1 AND P2_col == val2) OR (...)
        # Using Polars to generate the filter expression string or just iterate?
        
        # Iterating is safer for memory.
        from pyiceberg.expressions import EqualTo, And
        
        rows = distinct_partitions.to_dicts()
        for row in rows:
            # 3a. Build Partition Filter
            part_filter = AlwaysTrue()
            for col, val in row.items():
                # Note: Handling types (date, etc) might be tricky if not careful.
                # Assuming simple types for now.
                 if part_filter == AlwaysTrue():
                     part_filter = EqualTo(col, val)
                 else:
                     part_filter = And(part_filter, EqualTo(col, val))
            
            # 3b. Read Full Partition
            # We use the partition filter to get ALL rows in that partition
            part_arrow = table.scan(row_filter=part_filter).to_arrow()
            part_df = pl.from_arrow(part_arrow)
            
            # 3c. Apply Updates
            # We apply the user's update logic (which uses the mask)
            # The mask is based on the original filter.
            # We need to re-evaluate the mask against this partition df.
            # But the mask `expr.to_polars()` works on the df context.
            
            updated_part_df = part_df.with_columns(update_exprs)
            
            # 3d. Overwrite Partition
            # We overwrite only this partition
            try:
                 table.overwrite(updated_part_df.to_arrow(), overwrite_filter=part_filter)
            except TypeError:
                # Fallback if overwrite_filter not supported in version
                # But PyIceberg 0.6+ supports it. 
                # If checking IceFrame operations wrapper:
                # We need to call table.overwrite directly, bypassed operations wrapper if needed
                # or add support to operations.overwrite_table.
                # Here we are using table object directly.
                table.overwrite(updated_part_df.to_arrow(), overwrite_filter=part_filter)


    def merge(self, source_data: pl.DataFrame, on: str, 
              when_matched_update: Optional[Dict[str, Any]] = None,
              when_not_matched_insert: Optional[Dict[str, Any]] = None) -> None:
        """
        Merge source data into target table (Upsert).
        
        Simulated using Copy-on-Write:
        1. Read target
        2. Join with source
        3. Apply logic
        4. Overwrite
        """
        target_df = self.operations.read_table(self.table_name)
        
        # Perform join to identify matches
        # We'll do a full outer join to handle both matched and not matched
        # But Polars join might require suffix handling
        
        # Simpler approach:
        # 1. Identify IDs in source
        source_ids = source_data.select(on).unique()
        
        # 2. Split target into matched and not matched (if we can filter by ID)
        # Or just join and reconstruct
        
        # Let's use Polars update/join functionality
        # join(..., how="left") -> update columns
        # concat -> insert new
        
        # 1. Update existing rows
        if when_matched_update:
            # Join target with source on key
            # Replace columns in target with source columns where matched
            
            # This is complex to do efficiently in pure DataFrame API without SQL MERGE
            # Simplified:
            # A. Rows in target that are NOT in source -> Keep as is
            # B. Rows in target that ARE in source -> Update
            # C. Rows in source that are NOT in target -> Insert
            
            # A: Target Anti Join Source
            df_keep = target_df.join(source_data, on=on, how="anti")
            
            # B: Target Semi Join Source -> Update
            # Actually, we just take the source rows that match target rows?
            # If source has the updated values, we just take source rows that exist in target
            df_update = source_data.join(target_df.select(on), on=on, how="semi")
            
            # Apply specific updates if provided, otherwise assume source has full row?
            # If when_matched_update is a dict of col -> val/expr, we apply it
            # If it's just "update", we assume source replaces target
            
            if when_matched_update:
                # If specific updates, we might need to join to get old values if not updating all?
                # For simplicity, let's assume source contains the new state for updated rows
                pass
        else:
            # If no update, keep all target rows?
            # MERGE usually implies update or insert
            df_keep = target_df
            df_update = pl.DataFrame([], schema=target_df.schema)

        # 2. Insert new rows
        if when_not_matched_insert:
            # C: Source Anti Join Target
            df_insert = source_data.join(target_df, on=on, how="anti")
        else:
            df_insert = pl.DataFrame([], schema=target_df.schema)
            
        # Combine
        final_df = pl.concat([df_keep, df_update, df_insert])
        
        # Overwrite
        self.operations.overwrite_table(self.table_name, final_df)
