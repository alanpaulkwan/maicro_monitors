from modules.clickhouse_client import execute, query_df
import pandas as pd

try:
    print("Describing maicro_monitors.candles:")
    desc = execute("DESCRIBE TABLE maicro_monitors.candles")
    for col in desc:
        print(col)
    
    print("\nSample data (last 5 rows):")
    df = query_df("SELECT * FROM maicro_monitors.candles ORDER BY ts DESC LIMIT 5")
    print(df)
except Exception as e:
    print(f"Error: {e}")
