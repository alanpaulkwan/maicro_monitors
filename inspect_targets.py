from modules.clickhouse_client import execute, query_df
import pandas as pd

try:
    print("\nSample data with non-null weight:")
    df = query_df("SELECT * FROM maicro_logs.positions_jianan_v6 WHERE weight IS NOT NULL ORDER BY inserted_at DESC LIMIT 5")
    print(df)
    
    print("\nSample data with non-null weight_daily:")
    df2 = query_df("SELECT * FROM maicro_logs.positions_jianan_v6 WHERE weight_daily IS NOT NULL ORDER BY inserted_at DESC LIMIT 5")
    print(df2)
except Exception as e:
    print(f"Error: {e}")