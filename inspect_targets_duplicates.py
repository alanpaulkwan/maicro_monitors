from modules.clickhouse_client import execute, query_df
import pandas as pd

try:
    print("\nChecking for duplicate (date, symbol) pairs for date '2025-07-22':")
    df = query_df("SELECT date, symbol, count() as count FROM maicro_logs.positions_jianan_v6 WHERE date = '2025-07-22' GROUP BY date, symbol HAVING count > 1")
    print(df)

    print("\nChecking for duplicate (date, symbol) pairs for date '2025-01-06':")
    df2 = query_df("SELECT date, symbol, count() as count FROM maicro_logs.positions_jianan_v6 WHERE date = '2025-01-06' GROUP BY date, symbol HAVING count > 1")
    print(df2)
    
except Exception as e:
    print(f"Error: {e}")
