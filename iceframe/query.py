"""
Query Builder for IceFrame.

Provides a fluent API for building and executing queries on Iceberg tables.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Union

import polars as pl
from pyiceberg.expressions import AlwaysTrue

from iceframe.cache import (  # noqa: F401  (re-exported for backwards compatibility)
    QueryCache,
    get_query_cache,
    invalidate_query_cache,
    set_query_cache,
)
from iceframe.exceptions import ValidationError
from iceframe.expressions import Column, Expression, plan_pushdown
from iceframe.operations import TableOperations

logger = logging.getLogger(__name__)

#: Join strategies accepted by :meth:`QueryBuilder.join`. ``"outer"`` is kept as
#: a deprecated alias for Polars' modern ``"full"`` spelling.
_JOIN_HOWS = ("inner", "left", "right", "full", "outer", "semi", "anti", "cross")


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
        if how not in _JOIN_HOWS:
            raise ValidationError(
                f"Invalid join type: {how}. Must be one of: {', '.join(_JOIN_HOWS)}"
            )
        if how == "outer":
            # Polars >= 1.0 renamed "outer" to "full" and emits a
            # DeprecationWarning for the old spelling. Accept both, pass the
            # modern one through.
            logger.debug("join(how='outer') is deprecated; using how='full'")
            how = "full"

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

    def _cache_key_params(self, snapshot_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Stable, JSON-serialisable signature of this query for the cache.

        ``snapshot_id`` pins the entry to the table state it was computed from,
        so a write that produces a new snapshot can never be served a stale
        result even if invalidation is somehow missed.
        """
        def s(expr: Any) -> str:
            # __repr__ is deterministic for our small Expression tree; for
            # arbitrary user-supplied expressions we fall back to the type.
            try:
                return repr(expr)
            except Exception:
                return type(expr).__name__
        return {
            "select": [s(e) for e in self._select_exprs],
            "filters": [s(e) for e in self._filter_exprs],
            "group_by": [s(e) for e in self._group_by_exprs],
            "order_by": [s(e) for e in self._order_by_exprs],
            "limit": self._limit,
            "with_columns": [(n, s(e)) for n, e in self._with_columns],
            "joins": [(t, on, how) for (t, on, how) in self._joins],
            "snapshot_id": snapshot_id,
        }

    def _required_columns(self) -> Optional[Set[str]]:
        """
        The set of source-table columns this query actually needs, or ``None``
        when it can't be determined (which disables projection pushdown).
        """
        if not self._select_exprs:
            # No SELECT means "give me every column" — projecting down to just
            # the filter/order/with_column inputs would silently drop the rest
            # of the row from the result.
            return None

        needed: Set[str] = set()

        groups: List[Any] = list(self._select_exprs)
        groups += self._filter_exprs
        groups += self._group_by_exprs
        groups += self._order_by_exprs
        groups += [e for _, e in self._with_columns]

        for expr in groups:
            cols = expr.referenced_columns()
            if cols is None:
                return None
            needed |= cols

        # Join keys must survive the projection.
        for _, on, _how in self._joins:
            if isinstance(on, str):
                needed.add(on)
            elif isinstance(on, (list, tuple)):
                for c in on:
                    if not isinstance(c, str):
                        return None
                    needed.add(c)
            else:
                return None

        return needed

    def _plan_scan(self, table) -> Dict[str, Any]:
        """
        Build the kwargs for ``table.scan()``, deciding what can be pushed.

        Returns a dict with ``row_filter``/``selected_fields``/``limit`` plus a
        ``residual_filters`` list of predicates that still need local
        evaluation.
        """
        row_filter, residual = plan_pushdown(self._filter_exprs)

        # Projection pushdown: only when every expression's column set is known.
        selected_fields = ("*",)
        required = self._required_columns()
        if required is not None:
            table_cols = {f.name for f in table.schema().fields}
            unknown = required - table_cols
            if unknown:
                # A referenced name isn't a base column (e.g. it's produced by
                # with_column earlier in the plan). Don't risk projecting it away.
                logger.debug(
                    "Skipping projection pushdown for %s: unknown columns %s",
                    self.table_name,
                    sorted(unknown),
                )
            elif required:
                selected_fields = tuple(sorted(required))
            # `required` being empty (e.g. SELECT count(*)) means no column is
            # needed, but PyIceberg requires at least one field; keep "*".

        # Limit pushdown: only sound when the limit is the last thing that
        # happens. Any residual filter, join, aggregation, ordering or
        # projection that reorders/drops rows invalidates an early cap.
        scan_limit = None
        if (
            self._limit is not None
            and not residual
            and not self._joins
            and not self._group_by_exprs
            and not self._order_by_exprs
        ):
            scan_limit = self._limit

        return {
            "row_filter": row_filter,
            "selected_fields": selected_fields,
            "limit": scan_limit,
            "residual_filters": residual,
        }

    def execute(self) -> pl.DataFrame:
        """Execute the query and return a Polars DataFrame."""
        # 1. Plan the scan: predicate + projection + limit pushdown.
        table = self.operations.get_table(self.table_name)

        # 0. Cache lookup. ``cache(ttl)`` is a no-op without this — for years
        # the TTL was just stored on the builder and never consulted. We key
        # on table + query plan + current snapshot id, so two filter/limit
        # combos don't collide and a post-write read can't hit a pre-write entry.
        cache_enabled = self._cache_ttl is not None
        cache_params = None
        if cache_enabled:
            snap = table.current_snapshot()
            cache_params = self._cache_key_params(snap.snapshot_id if snap else None)
            cached = get_query_cache().get(self.table_name, cache_params)
            if cached is not None:
                return cached

        plan = self._plan_scan(table)
        polars_filters = plan.pop("residual_filters")

        # 2. Read from Iceberg
        scan = table.scan(**plan)
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

            # ORDER BY after an aggregation refers to the aggregated frame.
            if self._order_by_exprs:
                df = df.sort([e.to_polars() for e in self._order_by_exprs])

        else:
            # SQL allows ORDER BY on a column that isn't in the SELECT list, so
            # sort BEFORE projecting it away. Sorting afterwards raised
            # ColumnNotFoundError for `select("id").order_by("g")`.
            if self._order_by_exprs:
                df = df.sort([e.to_polars() for e in self._order_by_exprs])

            if self._select_exprs:
                df = df.select([e.to_polars() for e in self._select_exprs])

        # Apply Limit
        if self._limit is not None:
            df = df.head(self._limit)

        if cache_enabled:
            get_query_cache().put(self.table_name, cache_params, df, ttl=self._cache_ttl)
        return df

    # Write Operations

    def insert(self, data: Union[pl.DataFrame, Dict[str, List[Any]]]) -> None:
        """Insert data into the table"""
        self.operations.append_to_table(self.table_name, data)
        invalidate_query_cache(self.table_name)

    def delete(self) -> None:
        """
        Delete rows matching the filter.

        Every predicate must be fully pushable to Iceberg — a partially pushed
        filter would delete a *superset* of the intended rows, so we refuse
        instead.
        """
        if not self._filter_exprs:
            raise ValidationError("DELETE requires a filter (use filter/where)")

        combined_filter, residual = plan_pushdown(self._filter_exprs)

        if residual or isinstance(combined_filter, AlwaysTrue):
            raise ValidationError(
                "DELETE requires filters that translate fully to Iceberg predicates. "
                "Expressions such as column-to-column comparisons cannot be pushed "
                "down, and deleting on a weaker predicate would remove extra rows."
            )

        table = self.operations.get_table(self.table_name)
        table.delete(combined_filter)
        invalidate_query_cache(self.table_name)

    def update(self, updates: Dict[str, Any]) -> None:
        """
        Update rows matching the filter, in place, via copy-on-write.

        Unpartitioned tables are rewritten wholesale. Partitioned tables use
        PyIceberg's native ``Transaction.dynamic_partition_overwrite`` when the
        touched partitions can be identified, which replaces every affected
        partition in a **single atomic commit** instead of one commit per
        partition.

        Args:
            updates: ``{column: value_or_polars_expr}`` applied to matching rows.
        """
        if not self._filter_exprs:
            raise ValidationError("UPDATE requires a filter")

        table = self.operations.get_table(self.table_name)
        spec = table.spec()

        # Build the row mask for Polars (always the full predicate — never the
        # pushed-down superset).
        mask = None
        for expr in self._filter_exprs:
            condition = expr.to_polars()
            mask = condition if mask is None else (mask & condition)

        ice_filter, residual = plan_pushdown(self._filter_exprs)

        def _value_expr(new_value: Any) -> pl.Expr:
            return new_value if isinstance(new_value, pl.Expr) else pl.lit(new_value)

        update_exprs = [
            pl.when(mask).then(_value_expr(new_value)).otherwise(pl.col(col_name)).alias(col_name)
            for col_name, new_value in updates.items()
        ]

        if not spec.fields:
            # Unpartitioned - full rewrite.
            logger.info("Table %s is not partitioned; performing full rewrite", self.table_name)
            df = self.operations.read_table(self.table_name)
            df = df.with_columns(update_exprs)
            self.operations.overwrite_table(self.table_name, df)
            invalidate_query_cache(self.table_name)
            return

        # Partitioned: find the affected partitions by reading only the
        # partition source columns of matching rows.
        schema = table.schema()
        source_col_names = [schema.find_field(f.source_id).name for f in spec.fields]

        affected_scan = table.scan(
            row_filter=ice_filter,
            selected_fields=tuple(source_col_names),
        )
        affected_df = pl.from_arrow(affected_scan.to_arrow())

        if residual and affected_df.height:
            # The pushed filter is a superset; we can't narrow the partition
            # list further without the non-partition columns, so we keep the
            # superset. Rewriting an untouched partition is a no-op data-wise
            # (the mask won't match any row there), just extra I/O.
            logger.debug(
                "UPDATE on %s has non-pushable predicates; partition set may be a superset",
                self.table_name,
            )

        if affected_df.height == 0:
            return  # nothing matched

        distinct_partitions = affected_df.unique()
        logger.info(
            "Updating %d partition(s) of %s", distinct_partitions.height, self.table_name
        )

        from pyiceberg.expressions import And, EqualTo, IsNull, Or

        def _eq_or_isnull(col_name: str, val: Any):
            # Iceberg has no EqualTo(col, None); use IsNull for null values.
            return IsNull(col_name) if val is None else EqualTo(col_name, val)

        def _partition_filter(row: Dict[str, Any]):
            part_filter = None
            for col_name, val in row.items():
                pred = _eq_or_isnull(col_name, val)
                part_filter = pred if part_filter is None else And(part_filter, pred)
            return part_filter if part_filter is not None else AlwaysTrue()

        rows = distinct_partitions.to_dicts()

        # Read every affected partition, apply the update, and commit them all
        # at once.
        all_partitions_filter = None
        for row in rows:
            pf = _partition_filter(row)
            all_partitions_filter = pf if all_partitions_filter is None else Or(
                all_partitions_filter, pf
            )

        part_df = pl.from_arrow(table.scan(row_filter=all_partitions_filter).to_arrow())
        updated_df = part_df.with_columns(update_exprs)

        try:
            with table.transaction() as txn:
                txn.dynamic_partition_overwrite(updated_df.to_arrow())
        except (AttributeError, NotImplementedError, ValueError) as e:
            # Older PyIceberg, or a spec dynamic overwrite can't express
            # (e.g. void transforms). Fall back to per-partition overwrites.
            logger.warning(
                "dynamic_partition_overwrite unavailable for %s (%s); "
                "falling back to per-partition overwrite",
                self.table_name,
                e,
            )
            for row in rows:
                pf = _partition_filter(row)
                part_arrow = table.scan(row_filter=pf).to_arrow()
                one_df = pl.from_arrow(part_arrow).with_columns(update_exprs)
                table.overwrite(one_df.to_arrow(), overwrite_filter=pf)

        invalidate_query_cache(self.table_name)

    def merge(self, source_data: pl.DataFrame, on: str,
              when_matched_update: Optional[Dict[str, Any]] = None,
              when_not_matched_insert: Optional[Dict[str, Any]] = None) -> None:
        """
        Merge source data into the target table (upsert) using Copy-on-Write.

        Strategy:
            1. Split target rows into "matched" (key present in source) and
               "unmatched" (key absent in source).
            2. For matched rows, apply ``when_matched_update``. The value of each
               entry can be a constant, a Polars expression, or a string naming
               a column in ``source_data`` (looked up via a left join on ``on``).
            3. For source rows whose key isn't already in the target, insert them
               if ``when_not_matched_insert`` is truthy.
            4. Concatenate keep + updated + inserted rows aligned to the target
               schema and overwrite the table.

        Args:
            source_data: Polars DataFrame of incoming rows.
            on: Column name to merge on.
            when_matched_update: Optional dict ``{target_col: value_or_expr_or_source_col}``.
                If a value is a string and that string is a column in
                ``source_data``, the source's value for the matched row is used.
                Pass ``True`` (or any truthy non-dict) to replace matched rows
                wholesale with the corresponding source row.
            when_not_matched_insert: Truthy to insert source rows without a match.
                A dict may be provided to project specific source columns into
                the target (other target columns become null).
        """
        # Fast path: a plain "update everything on match, insert on no match"
        # merge is exactly PyIceberg's native upsert, which rewrites only the
        # affected files in a single atomic commit instead of reading and
        # overwriting the whole table.
        if (
            when_matched_update is not None
            and not isinstance(when_matched_update, dict)
            and when_not_matched_insert
            and not isinstance(when_not_matched_insert, dict)
        ):
            try:
                self.operations.upsert(self.table_name, source_data, join_cols=[on])
                invalidate_query_cache(self.table_name)
                return
            except Exception as e:
                logger.warning(
                    "Native upsert failed for %s (%s); falling back to the "
                    "copy-on-write merge path",
                    self.table_name,
                    e,
                )

        target_df = self.operations.read_table(self.table_name)
        target_schema = target_df.schema
        target_cols = target_df.columns

        # A: target rows not in source — keep as-is.
        df_keep = target_df.join(source_data.select(on), on=on, how="anti")

        # B: target rows in source — apply updates.
        if when_matched_update:
            matched = target_df.join(source_data.select(on), on=on, how="semi")

            if isinstance(when_matched_update, dict):
                # Join in the source columns we need; suffix collisions with "_src".
                src_renames = {c: f"__src_{c}" for c in source_data.columns if c != on}
                source_renamed = source_data.rename(src_renames)
                joined = matched.join(source_renamed, on=on, how="left")

                update_exprs = []
                for col, value in when_matched_update.items():
                    if col not in target_cols:
                        raise ValidationError(
                            f"when_matched_update references unknown target column {col!r}"
                        )
                    if isinstance(value, pl.Expr):
                        expr = value
                    elif isinstance(value, str) and value in source_data.columns:
                        expr = pl.col(f"__src_{value}") if value != on else pl.col(on)
                    else:
                        expr = pl.lit(value)
                    update_exprs.append(expr.alias(col))

                df_update = joined.with_columns(update_exprs).select(target_cols)
            else:
                # Replace matched rows wholesale with the corresponding source row.
                df_update = source_data.join(matched.select(on), on=on, how="semi")
        else:
            df_update = pl.DataFrame(schema=target_schema)

        # C: source rows whose key isn't already in target — insert.
        if when_not_matched_insert:
            new_rows = source_data.join(target_df.select(on), on=on, how="anti")
            if isinstance(when_not_matched_insert, dict):
                # Project specified columns; fill any missing target columns with null.
                projected = {}
                for tgt_col, src_value in when_not_matched_insert.items():
                    if isinstance(src_value, str) and src_value in new_rows.columns:
                        projected[tgt_col] = pl.col(src_value)
                    elif isinstance(src_value, pl.Expr):
                        projected[tgt_col] = src_value
                    else:
                        projected[tgt_col] = pl.lit(src_value)
                df_insert = new_rows.with_columns(
                    [v.alias(k) for k, v in projected.items()]
                )
            else:
                df_insert = new_rows
        else:
            df_insert = pl.DataFrame(schema=target_schema)

        # Align each piece to the target schema (add missing columns as null,
        # drop extras, reorder). Concat then overwrite.
        def _align(df: pl.DataFrame) -> pl.DataFrame:
            if df.height == 0:
                return pl.DataFrame(schema=target_schema)
            missing = [c for c in target_cols if c not in df.columns]
            out = df
            if missing:
                out = out.with_columns([
                    pl.lit(None).cast(target_schema[c]).alias(c) for c in missing
                ])
            return out.select(target_cols)

        final_df = pl.concat(
            [_align(df_keep), _align(df_update), _align(df_insert)],
            how="vertical_relaxed",
        )
        self.operations.overwrite_table(self.table_name, final_df)
        invalidate_query_cache(self.table_name)
