import os
import sys
from clickhouse_driver import Client

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from config.settings import CLICKHOUSE_LOCAL_CONFIG, CLICKHOUSE_REMOTE_CONFIG, HYPERLIQUID_ADDRESSES

DEFAULT_ADDRESS = HYPERLIQUID_ADDRESSES[0]

def get_clients():
    clients = []
    try:
        clients.append(("LOCAL", Client(**CLICKHOUSE_LOCAL_CONFIG)))
    except Exception as e:
        print(f"Failed to init local client: {e}")
        
    try:
        clients.append(("REMOTE", Client(**CLICKHOUSE_REMOTE_CONFIG)))
    except Exception as e:
        print(f"Failed to init remote client: {e}")
    return clients

def migrate_db(name, client):
    print(f"--- Migrating {name} ---")
    
    db = "maicro_monitors"
    table = "tracking_error_multilag"
    old_table = f"{table}_old"
    
    # Check if table exists
    exists = client.execute(f"EXISTS TABLE {db}.{table}")[0][0]
    if not exists:
        print(f"Table {db}.{table} does not exist. Creating new...")
        # Just create it
        create_sql = f"""
        CREATE TABLE {db}.{table}
        (
            `date` Date,
            `strategy_id` String,
            `lag` Int8,
            `te` Float64,
            `target_date` Date,
            `timestamp` DateTime DEFAULT now(),
            `address` String
        )
        ENGINE = ReplacingMergeTree(timestamp)
        ORDER BY (date, strategy_id, lag, address)
        """
        client.execute(create_sql)
        print("Created.")
        return

    # Check if already migrated
    cols = client.execute(f"DESCRIBE TABLE {db}.{table}")
    col_names = [c[0] for c in cols]
    if "address" in col_names:
        print(f"Table {db}.{table} already has address column.")
        return

    print(f"Renaming {table} to {old_table}...")
    client.execute(f"RENAME TABLE {db}.{table} TO {db}.{old_table}")
    
    print(f"Creating new {table}...")
    create_sql = f"""
    CREATE TABLE {db}.{table}
    (
        `date` Date,
        `strategy_id` String,
        `lag` Int8,
        `te` Float64,
        `target_date` Date,
        `timestamp` DateTime DEFAULT now(),
        `address` String
    )
    ENGINE = ReplacingMergeTree(timestamp)
    ORDER BY (date, strategy_id, lag, address)
    """
    client.execute(create_sql)
    
    print(f"Backfilling data from {old_table}...")
    # Insert with default address
    insert_sql = f"""
    INSERT INTO {db}.{table} (date, strategy_id, lag, te, target_date, timestamp, address)
    SELECT date, strategy_id, lag, te, target_date, timestamp, '{DEFAULT_ADDRESS}'
    FROM {db}.{old_table}
    """
    client.execute(insert_sql)
    
    print(f"Dropping {old_table}...")
    client.execute(f"DROP TABLE {db}.{old_table}")
    print("Migration complete.")

def main():
    clients = get_clients()
    for name, client in clients:
        migrate_db(name, client)

if __name__ == "__main__":
    main()
