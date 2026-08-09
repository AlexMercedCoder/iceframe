"""
Table operations for CRUD functionality
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

from iceframe.cache import invalidate_query_cache
from iceframe.exceptions import CatalogError, SchemaError, ValidationError
from iceframe.utils import normalize_table_identifier

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover - imports for type annotations only
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.table.sorting import SortOrder

    from iceframe.expressions import Expression


def to_arrow_table(data: Union[pl.DataFrame, pa.Table, Dict[str, list]]) -> pa.Table:
    """Coerce the accepted write payload shapes into a PyArrow table."""
    if isinstance(data, pl.DataFrame):
        return data.to_arrow()
    if isinstance(data, pa.Table):
        return data
    if isinstance(data, dict):
        return pa.Table.from_pydict(data)
    raise ValidationError(f"Unsupported data type: {type(data)}")


def _resolve_snapshot_for_timestamp(table: Table, as_of_timestamp_ms: int) -> Optional[int]:
    """
    Resolve a millisecond-epoch timestamp to the snapshot id that was current
    at that point in time (the most recent snapshot with
    ``timestamp_ms <= as_of_timestamp_ms``).

    Returns ``None`` if no such snapshot exists (i.e. the timestamp predates the
    table's first commit).
    """
    candidate_id: Optional[int] = None
    candidate_ts: int = -1
    try:
        for snap in table.snapshots():
            ts = getattr(snap, "timestamp_ms", None)
            if ts is None:
                continue
            if ts <= as_of_timestamp_ms and ts > candidate_ts:
                candidate_ts = ts
                candidate_id = snap.snapshot_id
    except Exception:
        return None
    return candidate_id


class _FieldIdCounter:
    """Hands out unique, increasing Iceberg field ids for one schema build."""

    def __init__(self, start: int = 1):
        self._next = start

    def next(self) -> int:
        value = self._next
        self._next += 1
        return value


class TableOperations:
    """Handle table CRUD operations"""

    def __init__(self, catalog: Catalog):
        """
        Initialize TableOperations.

        Args:
            catalog: PyIceberg catalog instance
        """
        self.catalog = catalog

    def _convert_schema(self, schema: Union[Schema, pa.Schema, pl.DataFrame, Dict[str, Any]]) -> Schema:
        """
        Convert various schema formats to PyIceberg Schema.

        Args:
            schema: Schema in various formats

        Returns:
            PyIceberg Schema object
        """
        if isinstance(schema, Schema):
            return schema

        if isinstance(schema, pa.Schema):
            # Convert PyArrow schema to PyIceberg schema
            return Schema(*self._pyarrow_to_iceberg_fields(schema))

        if isinstance(schema, pl.DataFrame):
            # Infer schema from Polars DataFrame
            return self._convert_schema(schema.to_arrow().schema)

        if isinstance(schema, dict):
            # Dict form. Each value may be:
            #   * a type string  -> {"id": "long"}
            #   * a PyIceberg type instance
            #   * a dict {"type": ..., "required": bool}  (nested/required form)
            counter = _FieldIdCounter()
            fields = []
            for name, spec in schema.items():
                field_id = counter.next()
                required = False
                if isinstance(spec, dict):
                    if "type" not in spec:
                        raise SchemaError(
                            f"Field {name!r} declared as a dict must contain a 'type' key"
                        )
                    required = bool(spec.get("required", False))
                    spec = spec["type"]

                if isinstance(spec, str):
                    field_type = self._string_to_iceberg_type(spec, counter)
                elif isinstance(spec, dict):
                    # Nested struct written inline as a dict of subfields.
                    sub_fields = []
                    for sub_name, sub_spec in spec.items():
                        sub_id = counter.next()
                        sub_required = False
                        if isinstance(sub_spec, dict) and "type" in sub_spec:
                            sub_required = bool(sub_spec.get("required", False))
                            sub_spec = sub_spec["type"]
                        sub_fields.append(
                            NestedField(
                                field_id=sub_id,
                                name=sub_name,
                                field_type=self._string_to_iceberg_type(sub_spec, counter),
                                required=sub_required,
                            )
                        )
                    field_type = StructType(*sub_fields)
                else:
                    # Assume it's already a PyIceberg type instance.
                    field_type = spec

                fields.append(
                    NestedField(
                        field_id=field_id,
                        name=name,
                        field_type=field_type,
                        required=required,
                    )
                )
            return Schema(*fields)

        raise SchemaError(f"Unsupported schema type: {type(schema)}")

    def _pyarrow_to_iceberg_fields(self, pa_schema: pa.Schema) -> List[NestedField]:
        """Convert PyArrow schema fields to PyIceberg fields.

        Field ids are allocated from a single monotonically increasing counter
        so that nested list/map/struct element ids never collide with top-level
        ids.
        """
        counter = _FieldIdCounter()
        fields = []
        for field in pa_schema:
            field_id = counter.next()
            iceberg_type = self._pyarrow_to_iceberg_type(field.type, counter)
            fields.append(
                NestedField(
                    field_id=field_id,
                    name=field.name,
                    field_type=iceberg_type,
                    required=not field.nullable,
                )
            )
        return fields

    def _pyarrow_to_iceberg_type(self, pa_type, counter: Optional["_FieldIdCounter"] = None):
        """
        Convert a PyArrow type to a PyIceberg type.

        Covers the primitives plus decimal, binary, time, uuid and the nested
        list / map / struct types. Anything still unmapped emits a
        ``UserWarning`` and falls back to ``StringType`` — previously the
        fallback was silent, which produced schema-corruption-style bugs that
        only showed up at read time.
        """
        if counter is None:
            counter = _FieldIdCounter(start=1000)

        if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            return StringType()
        elif pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int32(pa_type):
            return IntegerType()
        elif pa.types.is_int64(pa_type):
            return LongType()
        elif pa.types.is_float32(pa_type):
            return FloatType()
        elif pa.types.is_float64(pa_type):
            return DoubleType()
        elif pa.types.is_boolean(pa_type):
            return BooleanType()
        elif pa.types.is_decimal(pa_type):
            return DecimalType(pa_type.precision, pa_type.scale)
        elif pa.types.is_timestamp(pa_type):
            return TimestamptzType() if pa_type.tz else TimestampType()
        elif pa.types.is_time(pa_type):
            return TimeType()
        elif pa.types.is_date(pa_type):
            return DateType()
        elif pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
            return BinaryType()
        elif pa.types.is_fixed_size_binary(pa_type):
            # Arrow's 16-byte fixed binary is the canonical UUID encoding.
            return UUIDType() if pa_type.byte_width == 16 else BinaryType()
        elif pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type):
            element_id = counter.next()
            return ListType(
                element_id=element_id,
                element_type=self._pyarrow_to_iceberg_type(pa_type.value_type, counter),
                element_required=not pa_type.value_field.nullable,
            )
        elif pa.types.is_map(pa_type):
            key_id = counter.next()
            value_id = counter.next()
            return MapType(
                key_id=key_id,
                key_type=self._pyarrow_to_iceberg_type(pa_type.key_type, counter),
                value_id=value_id,
                value_type=self._pyarrow_to_iceberg_type(pa_type.item_type, counter),
                value_required=not pa_type.item_field.nullable,
            )
        elif pa.types.is_struct(pa_type):
            sub_fields = []
            for sub in pa_type:
                sub_id = counter.next()
                sub_fields.append(
                    NestedField(
                        field_id=sub_id,
                        name=sub.name,
                        field_type=self._pyarrow_to_iceberg_type(sub.type, counter),
                        required=not sub.nullable,
                    )
                )
            return StructType(*sub_fields)
        else:
            import warnings
            warnings.warn(
                f"PyArrow type {pa_type!r} has no direct Iceberg mapping in IceFrame; "
                "falling back to StringType. Define the schema explicitly to silence "
                "this warning.",
                UserWarning,
                stacklevel=3,
            )
            return StringType()

    #: Type names accepted in the dict schema form, including common aliases.
    _TYPE_ALIASES = {
        "string": StringType,
        "str": StringType,
        "utf8": StringType,
        "int": IntegerType,
        "int32": IntegerType,
        "integer": IntegerType,
        "long": LongType,
        "int64": LongType,
        "bigint": LongType,
        "float": FloatType,
        "float32": FloatType,
        "double": DoubleType,
        "float64": DoubleType,
        "boolean": BooleanType,
        "bool": BooleanType,
        "timestamp": TimestampType,
        "timestamptz": TimestamptzType,
        "time": TimeType,
        "date": DateType,
        "binary": BinaryType,
        "bytes": BinaryType,
        "uuid": UUIDType,
    }

    def _string_to_iceberg_type(self, type_str: str, counter: Optional["_FieldIdCounter"] = None):
        """
        Convert a string type name to a PyIceberg type.

        Supported: the primitive names and aliases in ``_TYPE_ALIASES``, plus
        parameterised forms ``decimal(p, s)``, ``list<T>``, and ``map<K, V>``.
        Unknown names emit a ``UserWarning`` before falling back to
        ``StringType`` — silently coercing used to hide typos and produce the
        wrong Iceberg schema.
        """
        import re
        import warnings

        if counter is None:
            counter = _FieldIdCounter(start=1000)

        key = type_str.strip().lower()

        if key in self._TYPE_ALIASES:
            return self._TYPE_ALIASES[key]()

        dec = re.fullmatch(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", key)
        if dec:
            return DecimalType(int(dec.group(1)), int(dec.group(2)))
        if key == "decimal":
            # Iceberg has no unparameterised decimal; pick a lossless-ish default
            # and say so rather than silently producing a string column.
            warnings.warn(
                "'decimal' needs a precision and scale; defaulting to decimal(38, 9). "
                "Write e.g. 'decimal(10, 2)' to be explicit.",
                UserWarning,
                stacklevel=3,
            )
            return DecimalType(38, 9)

        lst = re.fullmatch(r"(?:list|array)\s*<\s*(.+?)\s*>", key)
        if lst:
            return ListType(
                element_id=counter.next(),
                element_type=self._string_to_iceberg_type(lst.group(1), counter),
                element_required=False,
            )

        mp = re.fullmatch(r"map\s*<\s*(.+?)\s*,\s*(.+?)\s*>", key)
        if mp:
            return MapType(
                key_id=counter.next(),
                key_type=self._string_to_iceberg_type(mp.group(1), counter),
                value_id=counter.next(),
                value_type=self._string_to_iceberg_type(mp.group(2), counter),
                value_required=False,
            )

        warnings.warn(
            f"Unknown type string {type_str!r}; falling back to StringType. "
            f"Known types: {sorted(self._TYPE_ALIASES)} plus decimal(p, s), "
            "list<T> and map<K, V>.",
            UserWarning,
            stacklevel=3,
        )
        return StringType()

    @staticmethod
    def _build_sort_order(sort_order, iceberg_schema: Schema):
        """
        Build a real PyIceberg ``SortOrder`` from the documented list form.

        Accepts a list of entries, each of which may be:

        * ``"col"`` — ascending, nulls first (Iceberg's ascending default)
        * ``("col", "desc")`` / ``("col", "asc")``
        * ``("col", "desc", "nulls-last")``

        Previously this branch was a literal ``pass`` and the raw list was
        handed to PyIceberg, which raised
        ``AttributeError: 'list' object has no attribute 'is_unsorted'``.
        """
        from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
        from pyiceberg.transforms import IdentityTransform

        fields = []
        for entry in sort_order:
            null_order = None
            if isinstance(entry, str):
                col_name, direction = entry, "asc"
            elif isinstance(entry, (list, tuple)) and len(entry) in (1, 2, 3):
                col_name = entry[0]
                direction = entry[1] if len(entry) > 1 else "asc"
                if len(entry) > 2:
                    null_order = entry[2]
            else:
                raise SchemaError(
                    f"Invalid sort_order entry {entry!r}. Use 'col', ('col', 'desc'), "
                    "or ('col', 'desc', 'nulls-last')."
                )

            try:
                field = iceberg_schema.find_field(col_name)
            except ValueError as e:
                raise SchemaError(f"Sort column {col_name!r} not found in schema") from e
            if field is None:
                raise SchemaError(f"Sort column {col_name!r} not found in schema")

            direction_key = str(direction).lower()
            if direction_key in ("asc", "ascending"):
                sort_direction = SortDirection.ASC
                default_nulls = NullOrder.NULLS_FIRST
            elif direction_key in ("desc", "descending"):
                sort_direction = SortDirection.DESC
                default_nulls = NullOrder.NULLS_LAST
            else:
                raise SchemaError(
                    f"Invalid sort direction {direction!r} for column {col_name!r}; "
                    "expected 'asc' or 'desc'."
                )

            if null_order is None:
                resolved_nulls = default_nulls
            else:
                nulls_key = str(null_order).lower().replace("_", "-")
                if nulls_key in ("nulls-first", "first"):
                    resolved_nulls = NullOrder.NULLS_FIRST
                elif nulls_key in ("nulls-last", "last"):
                    resolved_nulls = NullOrder.NULLS_LAST
                else:
                    raise SchemaError(
                        f"Invalid null ordering {null_order!r} for column {col_name!r}; "
                        "expected 'nulls-first' or 'nulls-last'."
                    )

            fields.append(
                SortField(
                    source_id=field.field_id,
                    transform=IdentityTransform(),
                    direction=sort_direction,
                    null_order=resolved_nulls,
                )
            )

        return SortOrder(*fields)

    def create_table(
        self,
        table_name: str,
        schema: Union[Schema, pa.Schema, pl.DataFrame, Dict[str, Any]],
        partition_spec: Optional[Union[List[tuple], 'PartitionSpec']] = None,
        sort_order: Optional[Union[List[str], 'SortOrder']] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> Table:
        """
        Create a new Iceberg table.

        Args:
            table_name: Name of the table
            schema: Table schema
            partition_spec: Optional partition specification
            sort_order: Optional sort order
            properties: Optional table properties

        Returns:
            Created Table object
        """
        namespace, table = normalize_table_identifier(table_name)

        # Convert schema
        iceberg_schema = self._convert_schema(schema)

        # Create table
        full_table_name = f"{namespace}.{table}"

        # Ensure the namespace exists. "Already exists" is the expected case and
        # is ignored; anything else (auth, network, permissions) is surfaced as
        # a CatalogError instead of resurfacing later as a confusing
        # NoSuchTableError.
        from pyiceberg.exceptions import NamespaceAlreadyExistsError
        try:
            self.catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
        except Exception as e:
            if "already exists" in str(e).lower():
                pass
            else:
                raise CatalogError(
                    f"Could not create or verify namespace {namespace!r}: {e}"
                ) from e

        # Handle partition spec and sort order
        # If they are just passed as is, PyIceberg might expect specific objects.
        # Ensure we don't pass explicit None if that breaks things, or convert.

        create_kwargs = {
            "identifier": full_table_name,
            "schema": iceberg_schema,
            "properties": properties or {},
        }

        if partition_spec is not None:
             if isinstance(partition_spec, list):
                 from pyiceberg.partitioning import PartitionSpec
                 from pyiceberg.transforms import (
                     BucketTransform,
                     DayTransform,
                     HourTransform,
                     IdentityTransform,
                     MonthTransform,
                     TruncateTransform,
                     VoidTransform,
                     YearTransform,
                 )

                 # Manual construction if builder_for fails/not available
                 try:
                     from pyiceberg.partitioning import PartitionField, PartitionSpec

                     fields = []
                     field_id_counter = 1000

                     for col, transform_str in partition_spec:
                         field = iceberg_schema.find_field(col)
                         if not field:
                             raise SchemaError(f"Partition column {col} not found in schema")

                         transform = None
                         name = col # default name

                         if transform_str == "identity":
                             transform = IdentityTransform()
                         elif transform_str.startswith("bucket"):
                             import re
                             match = re.search(r"bucket\[(\d+)\]", transform_str)
                             if match:
                                 transform = BucketTransform(int(match.group(1)))
                                 name = f"bucket_{col}" # convention
                             else:
                                 transform = BucketTransform(16) # default
                         elif transform_str.startswith("truncate"):
                             match = re.search(r"truncate\[(\d+)\]", transform_str)
                             if match:
                                 transform = TruncateTransform(int(match.group(1)))
                                 name = f"truncate_{col}"
                             else:
                                 transform = TruncateTransform(16)
                         elif transform_str == "year":
                             transform = YearTransform()
                             name = f"{col}_year"
                         elif transform_str == "month":
                             transform = MonthTransform()
                             name = f"{col}_month"
                         elif transform_str == "day":
                             transform = DayTransform()
                             name = f"{col}_day"
                         elif transform_str == "hour":
                             transform = HourTransform()
                             name = f"{col}_hour"
                         elif transform_str == "void":
                             transform = VoidTransform()
                             name = f"{col}_null"
                         else:
                             # Default to identity if unknown? or error
                             logger.warning(
                                "Unknown partition transform %r; using identity", transform_str
                            )
                             transform = IdentityTransform()

                         fields.append(PartitionField(
                             source_id=field.field_id,
                             field_id=field_id_counter,
                             transform=transform,
                             name=name
                         ))
                         field_id_counter += 1

                     create_kwargs["partition_spec"] = PartitionSpec(fields=tuple(fields))

                 except Exception as e:
                     logger.warning("Failed to build partition spec: %s", e)
                     # Fallback
                     create_kwargs["partition_spec"] = partition_spec

             else:
                 create_kwargs["partition_spec"] = partition_spec

        if sort_order is not None:
             if isinstance(sort_order, (list, tuple)):
                 create_kwargs["sort_order"] = self._build_sort_order(
                     sort_order, iceberg_schema
                 )
             else:
                 create_kwargs["sort_order"] = sort_order

        # Create the table.
        # NOTE: We do NOT auto-append data here even if the caller passed a DataFrame /
        # PyArrow Table as the `schema` argument. The schema is inferred and that's it.
        # Doing the append here as well caused a long-standing double-write bug because
        # every `create_table_from_*` helper does `create_table(schema=df)` *and then*
        # calls `append_to_table(...)`, which appended the data a second time.
        # Callers that want the data written should call `append_to_table` themselves.
        return self.catalog.create_table(**create_kwargs)


    def get_table(self, table_name: str) -> Table:
        """Get a table by name"""
        namespace, table = normalize_table_identifier(table_name)
        return self.catalog.load_table(f"{namespace}.{table}")

    def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[Union[str, "Expression"]] = None,
        limit: Optional[int] = None,
        snapshot_id: Optional[int] = None,
        as_of_timestamp: Optional[int] = None,
        filter: Optional["Expression"] = None,
        filter_sql: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Read data from a table.

        **Two filter dialects, named explicitly.** ``filter_expr`` accepted both
        and behaved completely differently depending on the argument's type —
        the same parameter name meant two languages with opposite pushdown
        behaviour. Prefer the explicit parameters:

        * ``filter=`` — an IceFrame :class:`~iceframe.expressions.Expression`,
          translated to an Iceberg predicate and **pushed into the scan**.
          Operands that can't be pushed are re-applied locally.
        * ``filter_sql=`` — a Polars SQL string, evaluated **locally** after
          the scan. No pushdown; the whole table (minus projection) is read.

        ``filter_expr`` still works and dispatches on type, but is deprecated.

        Args:
            table_name: Name of the table
            columns: Optional column selection (pushed into the scan)
            filter_expr: Deprecated. Expression -> pushed; str -> local Polars SQL.
            limit: Optional row limit
            snapshot_id: Optional snapshot ID for time travel
            as_of_timestamp: Optional millisecond epoch timestamp for time travel.
                Resolved to the most recent snapshot at or before the timestamp.
            filter: Iceberg-pushed predicate (preferred).
            filter_sql: Locally-evaluated Polars SQL predicate (preferred).

        Returns:
            Polars DataFrame
        """
        table = self.get_table(table_name)

        from pyiceberg.expressions import AlwaysTrue

        iceberg_filter = AlwaysTrue()
        polars_filter_str: Optional[str] = None
        polars_local_expr = None  # an IceFrame Expression that didn't push down

        if filter is not None and filter_expr is not None:
            raise ValidationError("Pass either `filter=` or `filter_expr=`, not both")
        if filter_sql is not None and isinstance(filter_expr, str):
            raise ValidationError("Pass either `filter_sql=` or `filter_expr=`, not both")

        if filter is not None:
            if isinstance(filter, str):
                raise ValidationError(
                    "`filter=` takes an IceFrame Expression (pushed to Iceberg). "
                    "For a local Polars SQL string, use `filter_sql=`."
                )
            filter_expr = filter
        if filter_sql is not None:
            if not isinstance(filter_sql, str):
                raise ValidationError(
                    "`filter_sql=` takes a Polars SQL string. For a pushed "
                    "Iceberg predicate, use `filter=`."
                )
            polars_filter_str = filter_sql

        if filter_expr is not None:
            if hasattr(filter_expr, "pushdown"):
                pushed, fully = filter_expr.pushdown()
                iceberg_filter = pushed
                if not fully:
                    # The pushed predicate is only a safe superset — keep the
                    # original Expression and re-apply it locally. Without this
                    # the unpushable operand would be silently dropped.
                    polars_local_expr = filter_expr
            elif hasattr(filter_expr, "to_iceberg"):
                # Foreign expression object without pushability tracking:
                # assume the worst and evaluate locally as well.
                pushed = filter_expr.to_iceberg()
                if isinstance(pushed, AlwaysTrue):
                    polars_local_expr = filter_expr
                else:
                    iceberg_filter = pushed
            elif isinstance(filter_expr, str):
                polars_filter_str = filter_expr

        # Resolve as_of_timestamp -> snapshot_id (PyIceberg's scan() takes
        # snapshot_id; older code attempted scan.use_ref(str(ms)) which is wrong
        # — use_ref takes a branch/tag name. We resolve via the metadata log.)
        if as_of_timestamp is not None and snapshot_id is None:
            snapshot_id = _resolve_snapshot_for_timestamp(table, as_of_timestamp)
            if snapshot_id is None:
                raise ValidationError(
                    f"No snapshot at or before timestamp {as_of_timestamp} (ms)"
                )

        # Only push `limit` into the scan when there is no local filter to apply
        # afterwards. Otherwise the scan would cap rows BEFORE the local filter
        # runs and we'd return fewer than `limit` matching rows.
        has_local_filter = polars_filter_str is not None or polars_local_expr is not None
        scan_limit = None if has_local_filter else limit

        scan = table.scan(
            row_filter=iceberg_filter,
            selected_fields=tuple(columns) if columns else ("*",),
            limit=scan_limit,
            snapshot_id=snapshot_id,
        )

        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)

        if polars_filter_str:
            df = df.filter(pl.sql_expr(polars_filter_str))
        if polars_local_expr is not None:
            df = df.filter(polars_local_expr.to_polars())

        # Final cap (also covers the case where the scan returned more rows than
        # `limit` due to pushdown granularity).
        if limit and df.height > limit:
            df = df.head(limit)

        return df

    def scan_batches(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[Union[str, 'Expression']] = None,
        limit: Optional[int] = None,
        snapshot_id: Optional[int] = None,
        as_of_timestamp: Optional[int] = None,
        batch_size: Optional[int] = None,
    ):
        """
        Scan table and return an iterator of PyArrow RecordBatches.

        Args:
            table_name: Name of the table
            columns: Optional column selection
            filter_expr: Optional filter expression (Expression object for pushdown recommended)
            limit: Optional row limit
            snapshot_id: Optional snapshot ID
            as_of_timestamp: Optional timestamp
            batch_size: Optional batch size hint

        Returns:
            Iterator of PyArrow RecordBatches
        """
        table = self.get_table(table_name)
        from pyiceberg.expressions import AlwaysTrue

        iceberg_filter = AlwaysTrue()
        if filter_expr is not None and hasattr(filter_expr, "pushdown"):
            pushed, fully = filter_expr.pushdown()
            iceberg_filter = pushed
            if not fully:
                # Batch streaming can't apply a residual predicate for the
                # caller without materialising, so be explicit rather than
                # silently returning extra rows.
                raise ValidationError(
                    "scan_batches() requires a filter that pushes down fully to "
                    "Iceberg. This expression cannot (e.g. a column-to-column "
                    "comparison); use read_table() instead, which applies the "
                    "residual predicate locally."
                )
        elif filter_expr is not None and hasattr(filter_expr, "to_iceberg"):
            pushed = filter_expr.to_iceberg()
            if not isinstance(pushed, AlwaysTrue):
                iceberg_filter = pushed

        # Resolve as_of_timestamp -> snapshot_id (PyIceberg's scan() does NOT
        # accept an `as_of_timestamp` kwarg; passing it raises).
        if as_of_timestamp is not None and snapshot_id is None:
            snapshot_id = _resolve_snapshot_for_timestamp(table, as_of_timestamp)
            if snapshot_id is None:
                raise ValidationError(
                    f"No snapshot at or before timestamp {as_of_timestamp} (ms)"
                )

        scan = table.scan(
            row_filter=iceberg_filter,
            selected_fields=tuple(columns) if columns else ("*",),
            limit=limit,
            snapshot_id=snapshot_id,
        )

        # Note: PyIceberg's to_arrow_batch_reader() returns a pa.RecordBatchReader
        # which is an iterator of RecordBatches
        return scan.to_arrow_batch_reader()

    def append_to_table(
        self,
        table_name: str,
        data: Union[pl.DataFrame, pa.Table, Dict[str, list]],
        branch: Optional[str] = None,
    ) -> None:
        """
        Append data to a table.

        Args:
            table_name: Name of the table
            data: Data to append
            branch: Optional branch name to write to
        """
        table = self.get_table(table_name)
        arrow_data = to_arrow_table(data)

        # Note: PyIceberg's append API might not directly support 'branch' arg in all versions
        # If supported, we pass it. If not, we might need to set WAP properties.
        try:
            # Try passing branch if supported by PyIceberg version
            if branch:
                # Check if append supports branch argument (newer PyIceberg)
                import inspect
                sig = inspect.signature(table.append)
                if 'branch' in sig.parameters:
                    table.append(arrow_data, branch=branch)
                    invalidate_query_cache(table_name)
                    return

                # Fallback: Use WAP properties if branch arg not supported
                # This sets write.wap.enabled=true and write.wap.id=<branch>
                with table.transaction() as txn:
                    txn.set_properties({
                        "write.wap.enabled": "true",
                        "write.wap.id": branch
                    })
                    txn.append(arrow_data)
                invalidate_query_cache(table_name)
                return

            table.append(arrow_data)
        except TypeError:
            # Fallback for older versions
            table.append(arrow_data)

        invalidate_query_cache(table_name)

    def overwrite_table(
        self,
        table_name: str,
        data: Union[pl.DataFrame, pa.Table, Dict[str, list]],
        overwrite_filter=None,
    ) -> None:
        """
        Overwrite table data.

        Args:
            table_name: Name of the table.
            overwrite_filter: Optional Iceberg predicate (or IceFrame
                ``Expression``) scoping the overwrite. When omitted the whole
                table is replaced.
        """
        table = self.get_table(table_name)
        arrow_data = to_arrow_table(data)

        if overwrite_filter is None:
            table.overwrite(arrow_data)
        else:
            if hasattr(overwrite_filter, "pushdown"):
                pushed, fully = overwrite_filter.pushdown()
                if not fully:
                    raise ValidationError(
                        "overwrite_filter must translate fully to an Iceberg "
                        "predicate; a partially-pushed filter would delete rows "
                        "the caller did not intend to replace."
                    )
                overwrite_filter = pushed
            table.overwrite(arrow_data, overwrite_filter=overwrite_filter)

        invalidate_query_cache(table_name)

    def upsert(
        self,
        table_name: str,
        data: Union[pl.DataFrame, pa.Table, Dict[str, list]],
        join_cols: Optional[List[str]] = None,
        when_matched_update_all: bool = True,
        when_not_matched_insert_all: bool = True,
    ) -> Dict[str, int]:
        """
        Upsert (MERGE) rows into a table using PyIceberg's native
        ``Table.upsert``.

        This rewrites only the data files that actually contain matching rows
        and commits once, unlike the copy-on-write
        :meth:`QueryBuilder.merge` fallback, which reads and overwrites the
        entire table.

        Args:
            table_name: Target table.
            data: Source rows.
            join_cols: Columns forming the match key. Defaults to the table's
                identifier fields.
            when_matched_update_all: Update matched rows from the source.
            when_not_matched_insert_all: Insert source rows with no match.

        Returns:
            ``{"rows_updated": int, "rows_inserted": int}``
        """
        table = self.get_table(table_name)
        arrow_data = to_arrow_table(data)

        kwargs: Dict[str, Any] = {
            "when_matched_update_all": when_matched_update_all,
            "when_not_matched_insert_all": when_not_matched_insert_all,
        }
        if join_cols:
            kwargs["join_cols"] = list(join_cols)

        result = table.upsert(arrow_data, **kwargs)
        invalidate_query_cache(table_name)
        return {
            "rows_updated": getattr(result, "rows_updated", 0),
            "rows_inserted": getattr(result, "rows_inserted", 0),
        }

    def transaction(self, table_name: str):
        """
        Return the PyIceberg ``Transaction`` context manager for a table.

        Everything staged inside the ``with`` block lands in one atomic commit::

            with ops.transaction("db.t") as txn:
                txn.set_properties({"owner": "data-eng"})
                txn.append(arrow_table)
        """
        return self.get_table(table_name).transaction()

    def inspect(self, table_name: str):
        """Return PyIceberg's metadata-table inspector for a table."""
        return self.get_table(table_name).inspect

    def delete_from_table(self, table_name: str, filter_expr: Union[str, Any]) -> None:
        """
        Delete rows from a table.

        ``filter_expr`` is an **Iceberg** predicate: either a string in
        Iceberg's expression dialect (``"id < 100"``) or an IceFrame
        ``Expression``. Note this differs from ``read_table(filter_expr=...)``,
        where a *string* is Polars SQL evaluated locally — see
        :meth:`read_table` and the ``filter=``/``filter_sql=`` parameters that
        name the two dialects explicitly.
        """
        table = self.get_table(table_name)

        if hasattr(filter_expr, "pushdown"):
            pushed, fully = filter_expr.pushdown()
            if not fully:
                raise ValidationError(
                    "delete_from_table() requires a filter that translates fully "
                    "to an Iceberg predicate; deleting on a weaker predicate "
                    "would remove extra rows."
                )
            filter_expr = pushed

        table.delete(filter_expr)
        invalidate_query_cache(table_name)


    def drop_table(self, table_name: str) -> None:
        """Drop a table"""
        namespace, table = normalize_table_identifier(table_name)
        self.catalog.drop_table(f"{namespace}.{table}")

    def list_tables(self, namespace: str = "default") -> List[str]:
        """
        List all tables in a namespace.

        Names are returned in IceFrame's own dotted form
        (``"namespace.table"``). Previously this returned ``str(tuple)`` —
        literally ``"('default', 'events')"`` — which could not be passed back
        into any other IceFrame method.

        Returns an empty list if the namespace does not exist. Connection or
        permission errors are surfaced rather than silently masked as empty —
        catching them here used to hide real catalog problems behind a
        confusingly empty response.
        """
        from pyiceberg.exceptions import NoSuchNamespaceError
        try:
            tables = self.catalog.list_tables(namespace)
        except NoSuchNamespaceError:
            return []
        return [".".join(t) if isinstance(t, tuple) else str(t) for t in tables]

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists. Returns False only when the catalog reports the
        table or namespace is missing; other errors (auth, network, ...) bubble
        up so they are visible to the caller.
        """
        from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
        try:
            self.get_table(table_name)
            return True
        except (NoSuchTableError, NoSuchNamespaceError):
            return False
