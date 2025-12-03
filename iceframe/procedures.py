"""
Stored procedures for maintenance tasks.
"""

from typing import Dict, Any, Optional
from pyiceberg.table import Table

from iceframe.compaction import CompactionManager
from iceframe.gc import GarbageCollector

class StoredProcedures:
    """
    Interface for calling maintenance procedures (Spark-like).
    """
    
    def __init__(self, table: Table):
        self.table = table
        self.compaction = CompactionManager(table)
        self.gc = GarbageCollector(table)
        
    def call(self, procedure_name: str, **kwargs) -> Any:
        """
        Call a stored procedure.
        
        Args:
            procedure_name: Name of procedure
            **kwargs: Arguments
            
        Returns:
            Result of procedure
        """
        proc = procedure_name.lower()
        
        if proc == "rewrite_data_files":
            return self.compaction.bin_pack(**kwargs)
            
        elif proc == "expire_snapshots":
            return self.gc.expire_snapshots(**kwargs)
            
        elif proc == "remove_orphan_files":
            return self.gc.remove_orphan_files(**kwargs)
            
        elif proc == "fast_forward":
            # Requires BranchManager logic
            from iceframe.branching import BranchManager
            bm = BranchManager(self.table)
            return bm.fast_forward(kwargs.get("branch", "main"), kwargs.get("to_branch"))
            
        else:
            raise ValueError(f"Unknown procedure: {procedure_name}")
