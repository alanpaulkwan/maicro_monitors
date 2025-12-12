import os
import sys
import pandas as pd
from clickhouse_driver import Client

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from config.settings import CLICKHOUSE_LOCAL_CONFIG, CLICKHOUSE_REMOTE_CONFIG

def get_clients():
    try:
        local = Client(**CLICKHOUSE_LOCAL_CONFIG)
    except Exception as e:
        print(f"Failed to init local client: {e}")
        local = None
        
    try:
        remote = Client(**CLICKHOUSE_REMOTE_CONFIG)
    except Exception as e:
        print(f"Failed to init remote client: {e}")
        remote = None
    return local, remote

def check_tables(client, name):
    if not client:
        print(f"--- {name} Client Missing ---")
        return

    print(f"--- Checking {name} ---")
    
    # Check if maicro_accounts exists (user mentioned it)
    dbs = client.execute("SHOW DATABASES")
    db_list = [d[0] for d in dbs]
    print(f"Databases found: {db_list}")
    
    target_db = "maicro_monitors"
    if target_db not in db_list:
        print(f"Database {target_db} not found!")
        return

    tables = client.execute(f"SHOW TABLES FROM {target_db}")
    table_list = [t[0] for t in tables]
    
    # Tables that MUST have address
    required_tables = [
        "account_snapshots",
        "positions_snapshots",
        "trades",
        "orders",
        "funding_payments",
        "ledger_updates",
        "tracking_error_multilag" 
    ]

    for t in required_tables:
        if t not in table_list:
            print(f"  [MISSING] Table {t} not found.")
            continue
            
        cols = client.execute(f"DESCRIBE TABLE {target_db}.{t}")
        col_names = [c[0] for c in cols]
        
        has_address = "address" in col_names
        status = "OK" if has_address else "FAIL - Missing 'address' column"
        print(f"  {t:<25} : {status}")
        
        # Check sorting key for a few critical ones to ensure address is included
        if has_address and t in ["trades", "orders", "positions_snapshots"]:
            create_query = client.execute(f"SHOW CREATE TABLE {target_db}.{t}")[0][0]
            if "ORDER BY" in create_query:
                order_by = create_query.split("ORDER BY")[1].split(")")[0].strip()
                # It might be complex, just printing it for manual review or simple check
                print(f"    ORDER BY: {order_by}")

def main():
    local, remote = get_clients()
    check_tables(local, "LOCAL")
    print("\n")
    check_tables(remote, "REMOTE")

if __name__ == "__main__":
    main()
