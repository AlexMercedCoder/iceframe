"""
Streaming support for IceFrame.
"""

import time
from collections.abc import Callable
from typing import Any

import polars as pl


class StreamingWriter:
    """
    Stream data to Iceberg tables with micro-batching.
    """

    def __init__(
        self,
        ice_frame,
        table_name: str,
        batch_size: int = 1000,
        flush_interval_seconds: float = 60,
        on_flush: Callable[[int], None] | None = None,
    ):
        """
        Initialize streaming writer.

        Args:
            ice_frame: IceFrame instance
            table_name: Target table name
            batch_size: Number of records per batch
            flush_interval_seconds: Max time between flushes
            on_flush: Called with the number of rows after each successful
                append, for example to commit source offsets
        """
        self.ice_frame = ice_frame
        self.table_name = table_name
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.on_flush = on_flush
        self._buffer: list[dict[str, Any]] = []
        self._last_flush = time.time()

        # Auto-compaction settings
        self.auto_compact = False
        self.compact_every_n_flushes = 10
        self._flushes_since_compact = 0

    def enable_auto_compaction(self, every_n_flushes: int = 10):
        """
        Enable auto-compaction.

        Args:
            every_n_flushes: Run compaction after this many flushes
        """
        if every_n_flushes < 1:
            raise ValueError("every_n_flushes must be >= 1")
        self.auto_compact = True
        self.compact_every_n_flushes = every_n_flushes

    def write(self, record: dict[str, Any]):
        """
        Write a single record.

        Args:
            record: Dictionary representing a row
        """
        self._buffer.append(record)

        if len(self._buffer) >= self.batch_size:
            self.flush()
        else:
            self.flush_if_due()

    def flush_if_due(self):
        """Flush if the flush interval has elapsed, even when no new record arrived."""
        if time.time() - self._last_flush >= self.flush_interval:
            self.flush()

    def flush(self):
        """Flush buffered records to table"""
        if not self._buffer:
            self._last_flush = time.time()
            return

        rows = len(self._buffer)
        df = pl.DataFrame(self._buffer)
        self.ice_frame.append_to_table(self.table_name, df)
        self._buffer = []
        self._last_flush = time.time()

        if self.on_flush is not None:
            self.on_flush(rows)

        self._flushes_since_compact += 1

        if self.auto_compact and self._flushes_since_compact >= self.compact_every_n_flushes:
            self._run_compaction()

    def _run_compaction(self):
        """Run compaction job"""
        try:
            self.ice_frame.compact_data_files(self.table_name)
        except Exception as exc:
            import logging

            logging.getLogger(__name__).exception("Auto-compaction failed for %s", self.table_name)
            raise RuntimeError(f"Auto-compaction failed for {self.table_name}: {exc}") from exc
        finally:
            self._flushes_since_compact = 0

    def close(self):
        """Close writer and flush remaining records"""
        self.flush()


def stream_from_kafka(
    ice_frame,
    kafka_topic: str,
    table_name: str,
    kafka_config: dict[str, Any],
    batch_size: int = 1000,
    flush_interval_seconds: float = 60,
    max_records: int | None = None,
    idle_timeout_seconds: float | None = None,
    poll_timeout_ms: int = 1000,
) -> int:
    """
    Stream JSON records from a Kafka topic into an Iceberg table.

    Delivery is at-least-once: offsets are committed only after the records
    they cover have been appended, so a crash can replay records but cannot
    lose them. Auto-commit is always disabled for that reason, and
    ``kafka_config`` needs a ``group_id`` for commits to be stored.

    Args:
        ice_frame: IceFrame instance
        kafka_topic: Kafka topic to consume from
        table_name: Target Iceberg table
        kafka_config: Kafka consumer configuration (``bootstrap_servers``,
            ``group_id``, ``auto_offset_reset`` and so on)
        batch_size: Records per append
        flush_interval_seconds: Longest a record waits in the buffer,
            including when the topic goes quiet
        max_records: Stop after this many records
        idle_timeout_seconds: Stop after this long without a new record
        poll_timeout_ms: How long each poll waits for records

    Returns:
        Number of records written
    """
    try:
        import json

        from kafka import KafkaConsumer
        from kafka.structs import OffsetAndMetadata
    except ImportError:
        raise ImportError(
            "kafka-python required. Install with: pip install 'iceframe[streaming]'"
        ) from None

    config = {**kafka_config, "enable_auto_commit": False}
    consumer = KafkaConsumer(
        kafka_topic, **config, value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    # Next offset to commit per partition, covering every record handed to the writer.
    offsets: dict[Any, int] = {}

    def commit(_rows: int) -> None:
        if offsets and config.get("group_id"):
            consumer.commit({tp: OffsetAndMetadata(o, "", -1) for tp, o in offsets.items()})

    writer = StreamingWriter(
        ice_frame,
        table_name,
        batch_size=batch_size,
        flush_interval_seconds=flush_interval_seconds,
        on_flush=commit,
    )

    written = 0
    last_record = time.time()
    try:
        while max_records is None or written < max_records:
            polled = consumer.poll(timeout_ms=poll_timeout_ms)
            for tp, messages in polled.items():
                for message in messages:
                    if max_records is not None and written >= max_records:
                        break
                    offsets[tp] = message.offset + 1
                    writer.write(message.value)
                    written += 1
            if polled:
                last_record = time.time()
            else:
                writer.flush_if_due()
                if (
                    idle_timeout_seconds is not None
                    and time.time() - last_record >= idle_timeout_seconds
                ):
                    break
    except KeyboardInterrupt:
        pass
    except BaseException:
        # Leave the buffered records unwritten and uncommitted: the next run of
        # this consumer group replays them. Retrying here could mask the error.
        consumer.close(autocommit=False)
        raise

    try:
        writer.close()
    finally:
        consumer.close(autocommit=False)

    return written
