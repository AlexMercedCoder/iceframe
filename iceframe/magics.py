"""
IPython Magics for IceFrame.

Provides %%iceql cell magic and %iceframe line magic.
"""

from IPython.core.magic import Magics, magics_class, line_magic, cell_magic
from IPython.core.display import display, HTML
import polars as pl

@magics_class
class IceFrameMagics(Magics):
    """Magics for IceFrame"""
    
    def __init__(self, shell):
        super().__init__(shell)
        self.active_iceframe = None
        
    @line_magic
    def iceframe(self, line):
        """
        Set or get the active IceFrame instance.
        
        Usage:
            %iceframe my_ice_instance
            %iceframe status
        """
        if not line or line == "status":
            if self.active_iceframe:
                print(f"Active IceFrame instance: {self.active_iceframe}")
                # Try to display connection info if available
                if hasattr(self.active_iceframe, 'catalog_config'):
                     print(f"Catalog: {self.active_iceframe.catalog_config.get('uri')}")
            else:
                print("No active IceFrame instance set. Use %iceframe <variable_name>")
            return

        # Try to get the variable from the user namespace
        var_name = line.strip()
        if var_name in self.shell.user_ns:
            obj = self.shell.user_ns[var_name]
            # Simple duck typing check or class check
            if hasattr(obj, 'read_table') and hasattr(obj, 'query'):
                self.active_iceframe = obj
                print(f"Set active IceFrame instance to: {var_name}")
            else:
                print(f"Variable '{var_name}' does not appear to be an IceFrame instance")
        else:
            print(f"Variable '{var_name}' not found in namespace")

    @cell_magic
    def iceql(self, line, cell):
        """
        Execute SQL query on the active IceFrame instance.
        
        Usage:
            %%iceql
            SELECT * FROM my_table LIMIT 10
        """
        if not self.active_iceframe:
            print("No active IceFrame instance. Set one with %iceframe <variable_name>")
            return
            
        query = cell.strip()
        if not query:
            return
            
        # For now, we'll use Polars SQL context if available or simple query parsing
        # IceFrame doesn't have a full SQL engine yet, but we can support basic SELECT
        # Or we can use Polars SQLContext on registered tables?
        
        # Strategy:
        # 1. Parse table name from FROM clause
        # 2. Read table into Polars
        # 3. Use Polars SQLContext
        
        try:
            # Simple SQL execution using Polars SQLContext
            ctx = pl.SQLContext()
            
            # We need to register tables referenced in the query
            # This is tricky without a full parser. 
            # Let's try to register all tables in the namespace? No, too expensive.
            # Let's rely on the user to have variables? No.
            
            # Alternative: Use IceFrame's query builder if the SQL is simple?
            # Or just implement a basic SQL runner that registers tables on demand?
            
            # Let's try to find table names in the query (naive approach)
            # Look for "FROM table_name" or "JOIN table_name"
            import re
            table_names = set(re.findall(r'FROM\s+([a-zA-Z0-9_.]+)', query, re.IGNORECASE))
            table_names.update(re.findall(r'JOIN\s+([a-zA-Z0-9_.]+)', query, re.IGNORECASE))
            
            for table_name in table_names:
                try:
                    # Read table (lazy if possible, but read_table returns DataFrame)
                    # We should add a read_table_lazy to IceFrame?
                    # For now, read eager
                    df = self.active_iceframe.read_table(table_name)
                    
                    # Register with SQLContext
                    # Handle namespace dots in table name for SQLContext?
                    # Polars SQLContext might not like dots in table names
                    safe_name = table_name.replace('.', '_')
                    ctx.register(safe_name, df)
                    
                    # Also register with original name if no dots, or just warn?
                    if safe_name != table_name:
                         # Replace in query?
                         query = query.replace(table_name, safe_name)
                         
                except Exception as e:
                    print(f"Warning: Could not load table '{table_name}': {e}")
            
            # Execute query
            result = ctx.execute(query).collect()
            display(result)
            
        except Exception as e:
            print(f"Error executing query: {e}")

def load_ipython_extension(ipython):
    """Load the extension in IPython"""
    ipython.register_magics(IceFrameMagics)
