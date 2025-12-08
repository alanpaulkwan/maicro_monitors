from modules.clickhouse_client import execute, query_df
import pandas as pd

try:
    print("Describing maicro_logs.live_account:")
    desc = execute("DESCRIBE TABLE maicro_logs.live_account")
    for col in desc:
        print(col)
    
    print("\nSample data (last 5 rows):")
    df = query_df("SELECT * FROM maicro_logs.live_account ORDER BY ts DESC LIMIT 5")
    print(df)
except Exception as e:
    print(f"Error: {e}")