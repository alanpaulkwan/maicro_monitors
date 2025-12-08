
import os
import sys
from clickhouse_driver import Client

# Add repo root
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import CLICKHOUSE_LOCAL_CONFIG, CLICKHOUSE_REMOTE_CONFIG

def create_table(config, name):
    print(f"[{name}] Creating tracking_error_multilag...")
    try:
        client = Client(**config)
        sql = """
        CREATE TABLE IF NOT EXISTS maicro_monitors.tracking_error_multilag (
            date Date,
            strategy_id String,
            lag Int8,
            te Float64,
            target_date Date,
            timestamp DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(timestamp)
        ORDER BY (date, strategy_id, lag)
        """
        client.execute(sql)
        print(f"[{name}] Done.")
    except Exception as e:
        print(f"[{name}] Error: {e}")

def main():
    create_table(CLICKHOUSE_LOCAL_CONFIG, "LOCAL")
    create_table(CLICKHOUSE_REMOTE_CONFIG, "REMOTE")

if __name__ == "__main__":
    main()
