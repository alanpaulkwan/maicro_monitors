from modules.clickhouse_client import execute, query_df
import pandas as pd

try:
    print("Describing maicro_monitors.tracking_error:")
    desc = execute("DESCRIBE TABLE maicro_monitors.tracking_error")
    for col in desc:
        print(col)
    
    print("\nSample data (last 5 rows):")
    df = query_df("SELECT * FROM maicro_monitors.tracking_error ORDER BY date DESC LIMIT 5")
    print(df)
except Exception as e:
    print(f"Error: {e}")
