
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env
import polars as pl

# Setup
config = load_catalog_config_from_env()
ice = IceFrame(config)
table_name = "test_txn_inspect"

# Create table if not exists
if not ice.table_exists(table_name):
    ice.create_table(table_name, pl.DataFrame({"id": [1]}))

table = ice.get_table(table_name)
txn = table.transaction()

print("Transaction attributes:")
print([a for a in dir(txn) if not a.startswith("__")])

# Check for _updates or similar
if hasattr(txn, "_updates"):
    print("\nHas _updates attribute")
    print(type(txn._updates))
elif hasattr(txn, "_requirements"):
    print("\nHas _requirements attribute")
