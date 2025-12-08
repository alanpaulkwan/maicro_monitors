
from modules.clickhouse_client import query_df
print(query_df("SELECT max(timestamp) FROM maicro_monitors.account_snapshots"))
print(query_df("SELECT count() FROM maicro_monitors.account_snapshots WHERE toDate(timestamp) = '2025-12-08'"))
