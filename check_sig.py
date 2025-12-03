
from pyiceberg.table import Table
print([m for m in dir(Table) if not m.startswith("_")])
