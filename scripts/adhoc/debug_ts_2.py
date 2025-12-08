
import sys, os
sys.path.append(os.getcwd())
from modules.clickhouse_client import query_df

print("Account Max TS:")
print(query_df("SELECT max(timestamp) FROM maicro_monitors.account_snapshots"))

print("Positions Max TS:")
print(query_df("SELECT max(timestamp) FROM maicro_monitors.positions_snapshots"))
