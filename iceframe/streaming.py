"""
Streaming support for IceFrame.
"""

from typing import Optional, Dict, Any
import polars as pl
import time

class StreamingWriter:
    """
    Stream data to Iceberg tables with micro-batching.
    """
    
    def __init__(
        self,
        ice_frame,
        table_name: str,
        batch_size: int = 1000,
        flush_interval_seconds: int = 60
    ):
        """
        Initialize streaming writer.
        
        Args:
            ice_frame: IceFrame instance
            table_name: Target table name
            batch_size: Number of records per batch
            flush_interval_seconds: Max time between flushes
        """
        self.ice_frame = ice_frame
        self.table_name = table_name
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self._buffer = []
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
        self.auto_compact = True
        self.compact_every_n_flushes = every_n_flushes
        
    def write(self, record: Dict[str, Any]):
        """
        Write a single record.
        
        Args:
            record: Dictionary representing a row
        """
        self._buffer.append(record)
        
        # Flush if batch size reached or interval elapsed
        if (len(self._buffer) >= self.batch_size or 
            time.time() - self._last_flush >= self.flush_interval):
            self.flush()
            
    def flush(self):
        """Flush buffered records to table"""
        if not self._buffer:
            return
            
        df = pl.DataFrame(self._buffer)
        self.ice_frame.append_to_table(self.table_name, df)
        self._buffer = []
        self._last_flush = time.time()
        
        self._flushes_since_compact += 1
        
        if self.auto_compact and self._flushes_since_compact >= self.compact_every_n_flushes:
            self._run_compaction()
            
    def _run_compaction(self):
        """Run compaction job"""
        try:
            # Assuming compaction module exists and is exposed via ice_frame.compaction
            # If not, we might need to import it or use operations directly
            # For now, we'll try to use the compaction feature module if available
            if hasattr(self.ice_frame, 'compaction'):
                self.ice_frame.compaction.bin_pack(self.table_name)
            else:
                # Fallback or log warning
                pass
        except Exception:
            # Don't fail streaming if compaction fails
            pass
        finally:
            self._flushes_since_compact = 0
        
    def close(self):
        """Close writer and flush remaining records"""
        self.flush()


def stream_from_kafka(
    ice_frame,
    kafka_topic: str,
    table_name: str,
    kafka_config: Dict[str, Any],
    batch_size: int = 1000
):
    """
    Stream data from Kafka to Iceberg table.
    
    Args:
        ice_frame: IceFrame instance
        kafka_topic: Kafka topic to consume from
        table_name: Target Iceberg table
        kafka_config: Kafka consumer configuration
        batch_size: Records per batch
    """
    try:
        from kafka import KafkaConsumer
        import json
    except ImportError:
        raise ImportError("kafka-python required. Install with: pip install 'iceframe[streaming]'")
        
    consumer = KafkaConsumer(
        kafka_topic,
        **kafka_config,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    writer = StreamingWriter(ice_frame, table_name, batch_size=batch_size)
    
    try:
        for message in consumer:
            writer.write(message.value)
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()
        consumer.close()
