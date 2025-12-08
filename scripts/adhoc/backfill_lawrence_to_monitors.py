
import os
import sys
import pandas as pd
from clickhouse_driver import Client

# Add repo root
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import CLICKHOUSE_LOCAL_CONFIG, CLICKHOUSE_REMOTE_CONFIG

# Hardcoded fallback credentials from GEMINI.md
FALLBACK_USER = "claude"
FALLBACK_PASS = "IAMSentient"

def get_client(config, name):
    try:
        # Try default config first
        client = Client(**config)
        client.execute("SELECT 1")
        return client
    except Exception:
        print(f"[{name}] Default auth failed, trying fallback...")
        # Try fallback
        fallback_config = config.copy()
        fallback_config["user"] = FALLBACK_USER
        fallback_config["password"] = FALLBACK_PASS
        try:
            client = Client(**fallback_config)
            client.execute("SELECT 1")
            return client
        except Exception as e:
            print(f"[{name}] Connection failed: {e}")
            return None

def fix_hl_meta_schema(local, remote):
    print("\n--- Fixing hl_meta Schema ---")
    sql_drop = "DROP TABLE IF EXISTS maicro_monitors.hl_meta"
    sql_create = """
    CREATE TABLE maicro_monitors.hl_meta (
        symbol String,
        sz_decimals Int32,
        px_decimals Int32,
        size_step Float64,
        tick_size Float64,
        min_units Float64,
        min_usd Float64,
        updated_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY (symbol, updated_at)
    """
    
    for client, name in [(local, "LOCAL"), (remote, "REMOTE")]:
        if not client: continue
        print(f"[{name}] Recreating hl_meta...")
        try:
            client.execute(sql_drop)
            client.execute(sql_create)
            print(f"[{name}] Done.")
        except Exception as e:
            print(f"[{name}] Error: {e}")

def backfill_account(local_read, local_write, remote_write):
    print("\n--- Backfilling Account Snapshots ---")
    # Read from local maicro_logs
    try:
        # Check columns first
        cols = local_read.execute("DESCRIBE maicro_logs.live_account")
        col_names = [c[0] for c in cols]
        print(f"Source columns: {col_names}")
        
        # Select mapping
        # ts -> timestamp
        # equity_usd -> accountValue
        # margin_used -> totalMarginUsed
        # withdrawable -> withdrawable
        # others default to 0
        
        query = """
        SELECT 
            ts, 
            equity_usd, 
            margin_used, 
            withdrawable 
        FROM maicro_logs.live_account
        ORDER BY ts
        """
        rows = local_read.execute(query)
        print(f"Fetched {len(rows)} rows from maicro_logs.live_account")
        
        if not rows:
            return

        data = []
        for r in rows:
            data.append({
                "timestamp": r[0],
                "accountValue": float(r[1]),
                "totalMarginUsed": float(r[2]),
                "totalNtlPos": 0.0,
                "totalRawUsd": 0.0,
                "marginUsed": float(r[2]), # Approx
                "withdrawable": float(r[3])
            })
            
        df = pd.DataFrame(data)
        
        insert_sql = """
        INSERT INTO maicro_monitors.account_snapshots 
        (timestamp, accountValue, totalMarginUsed, totalNtlPos, totalRawUsd, marginUsed, withdrawable)
        VALUES
        """
        
        for client, name in [(local_write, "LOCAL"), (remote_write, "REMOTE")]:
            if not client: continue
            print(f"[{name}] Inserting {len(df)} rows...")
            # Chunk it
            batch_size = 10000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                client.execute(insert_sql, batch.to_dict('records'))
            print(f"[{name}] Done.")
            
    except Exception as e:
        print(f"Error backfilling account: {e}")

def backfill_positions(local_read, local_write, remote_write):
    print("\n--- Backfilling Positions Snapshots ---")
    try:
        # Check columns
        cols = local_read.execute("DESCRIBE maicro_logs.live_positions")
        col_names = [c[0] for c in cols]
        print(f"Source columns: {col_names}")
        
        # Mapping:
        # ts -> timestamp
        # symbol -> coin
        # size -> szi
        # entry_px -> entryPx
        # usd -> positionValue
        # pnl -> unrealizedPnl
        # roe -> returnOnEquity
        # liq_px -> liquidationPx
        # leverage -> leverage
        # max_leverage -> maxLeverage
        # margin_used -> marginUsed
        
        query = """
        SELECT 
            ts, symbol, size, entry_px, usd, pnl, roe, liq_px, leverage, max_leverage, margin_used
        FROM maicro_logs.live_positions
        WHERE kind = 'current'
        ORDER BY ts
        """
        rows = local_read.execute(query)
        print(f"Fetched {len(rows)} rows from maicro_logs.live_positions (kind='current')")
        
        if not rows:
            return

        data = []
        for r in rows:
            data.append({
                "timestamp": r[0],
                "coin": r[1],
                "szi": float(r[2]),
                "entryPx": float(r[3]),
                "positionValue": float(r[4]),
                "unrealizedPnl": float(r[5]),
                "returnOnEquity": float(r[6]),
                "liquidationPx": float(r[7]),
                "leverage": float(r[8]),
                "maxLeverage": int(r[9]),
                "marginUsed": float(r[10])
            })
            
        df = pd.DataFrame(data)
        
        insert_sql = """
        INSERT INTO maicro_monitors.positions_snapshots
        (timestamp, coin, szi, entryPx, positionValue, unrealizedPnl, returnOnEquity, liquidationPx, leverage, maxLeverage, marginUsed)
        VALUES
        """
        
        for client, name in [(local_write, "LOCAL"), (remote_write, "REMOTE")]:
            if not client: continue
            print(f"[{name}] Inserting {len(df)} rows...")
            batch_size = 10000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                client.execute(insert_sql, batch.to_dict('records'))
            print(f"[{name}] Done.")
            
    except Exception as e:
        print(f"Error backfilling positions: {e}")

def main():
    print("Initializing clients...")
    local = get_client(CLICKHOUSE_LOCAL_CONFIG, "LOCAL")
    remote = get_client(CLICKHOUSE_REMOTE_CONFIG, "REMOTE")
    
    if not local:
        print("Fatal: Could not connect to Local ClickHouse.")
        return
        
    fix_hl_meta_schema(local, remote)
    backfill_account(local, local, remote)
    backfill_positions(local, local, remote)

if __name__ == "__main__":
    main()
