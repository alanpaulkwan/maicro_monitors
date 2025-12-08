
import sys, os
import pandas as pd
from datetime import datetime
sys.path.append(os.getcwd())
from modules.clickhouse_client import query_df

run_date = datetime.utcnow().date()
print(f"Run Date: {run_date}")

time_sql = """
    SELECT max(timestamp) as max_ts
    FROM maicro_monitors.account_snapshots
    WHERE toDate(timestamp) = %(d)s
"""
time_df = query_df(time_sql, params={"d": run_date})
print("Time DF:", time_df)

if not time_df.empty:
    max_ts = time_df.iloc[0]["max_ts"]
    print(f"Max TS: {max_ts} (type: {type(max_ts)})")
    
    pos_sql = """
        SELECT coin as symbol, positionValue, szi
        FROM maicro_monitors.positions_snapshots
        WHERE timestamp = %(ts)s
    """
    pos_df = query_df(pos_sql, params={"ts": max_ts})
    print(f"Pos DF: {len(pos_df)} rows")
    print(pos_df.head())
