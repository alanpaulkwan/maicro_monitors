
import os
import sys
from clickhouse_driver import Client

# Add repo root
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import CLICKHOUSE_LOCAL_CONFIG, CLICKHOUSE_REMOTE_CONFIG, HYPERLIQUID_ADDRESS

# Tables to migrate and their keys
TABLES = {
    "maicro_monitors.trades": "ORDER BY (coin, time, tid, address)",
    "maicro_monitors.orders": "ORDER BY (coin, timestamp, oid, address)",
    "maicro_monitors.account_snapshots": "ORDER BY (timestamp, address)",
    "maicro_monitors.positions_snapshots": "ORDER BY (coin, timestamp, address)",
    "maicro_monitors.funding_payments": "ORDER BY (coin, time, address)",
    "maicro_monitors.ledger_updates": "ORDER BY (time, hash, address)",
}

def migrate(config, name):
    print(f"[{name}] Starting migration for address: {HYPERLIQUID_ADDRESS}")
    try:
        client = Client(**config)
        
        for table, order_by in TABLES.items():
            print(f"  Migrating {table}...")
            
            # 1. Rename to _tmp
            try:
                client.execute(f"RENAME TABLE {table} TO {table}_tmp")
            except Exception as e:
                print(f"    Table {table} might not exist or already renamed: {e}")
                # Check if tmp exists, if not, maybe table doesn't exist at all
                continue

            # 2. Get CREATE statement of _tmp
            create_sql = client.execute(f"SHOW CREATE TABLE {table}_tmp")[0][0]
            
            # 3. Modify CREATE statement to include address
            # It usually looks like "CREATE TABLE ... (col Type, ...) ENGINE = ... ORDER BY ..."
            # We need to inject `address String` into columns and update ORDER BY
            
            # A safer way is to construct it manually or rely on the fact we know the schema.
            # But the schema in init_db.sql might be authoritative. 
            # Let's just use the columns from the tmp table and add address.
            
            # Actually, simpler:
            # CREATE TABLE {table} AS {table}_tmp ENGINE = ...
            # But we need to change structure.
            
            # Let's parse the columns from DESCRIBE
            cols = client.execute(f"DESCRIBE TABLE {table}_tmp")
            # cols is [(name, type, ...), ...]
            col_defs = []
            for c in cols:
                col_name = c[0]
                col_type = c[1]
                col_defs.append(f"{col_name} {col_type}")
            
            col_defs.append("address String")
            col_str = ", ".join(col_defs)
            
            # Extract Engine
            # We know the engines from init_db.sql usually.
            # ReplacingMergeTree() or MergeTree()
            # Let's just use what we know they are.
            engine = "ReplacingMergeTree()" 
            if "snapshots" in table:
                engine = "MergeTree()"
            
            create_new_sql = f"""
            CREATE TABLE {table} (
                {col_str}
            ) ENGINE = {engine}
            {order_by}
            """
            
            # 4. Create new table
            client.execute(create_new_sql)
            
            # 5. Insert data
            cols_old = [c[0] for c in cols]
            cols_old_str = ", ".join(cols_old)
            insert_sql = f"""
            INSERT INTO {table} ({cols_old_str}, address)
            SELECT {cols_old_str}, '{HYPERLIQUID_ADDRESS}'
            FROM {table}_tmp
            """
            client.execute(insert_sql)
            print(f"    Migrated data.")
            
            # 6. Drop tmp
            client.execute(f"DROP TABLE {table}_tmp")
            print(f"    Dropped tmp.")
            
    except Exception as e:
        print(f"[{name}] Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    migrate(CLICKHOUSE_LOCAL_CONFIG, "LOCAL")
    migrate(CLICKHOUSE_REMOTE_CONFIG, "REMOTE")

if __name__ == "__main__":
    main()
