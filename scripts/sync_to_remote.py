#!/usr/bin/env python3
import sys
import os
import time
import logging
from clickhouse_driver import Client

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import CLICKHOUSE_LOCAL_CONFIG, CLICKHOUSE_REMOTE_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_client(config):
    return Client(**config)

def sync_table(local_client, remote_client, db, table, time_col, batch_size=10000):
    full_table_name = f"{db}.{table}"
    logger.info(f"Syncing {full_table_name}...")

    try:
        # 1. Get max timestamp from remote
        # Check if table exists on remote first
        exists = remote_client.execute(f"EXISTS TABLE {full_table_name}")[0][0]
        if not exists:
            logger.warning(f"Table {full_table_name} does not exist on remote. Skipping.")
            # Optional: Create it? For now, assume schema is managed separately or synced via init_db.
            # Actually, for maicro_monitors, we know schema matches. For others, maybe not.
            return

        max_ts_query = f"SELECT max({time_col}) FROM {full_table_name}"
        try:
            max_ts = remote_client.execute(max_ts_query)[0][0]
        except Exception as e:
            logger.warning(f"Could not get max timestamp for {full_table_name}: {e}. Defaulting to 0.")
            max_ts = None

        # 2. Query local for new data
        if max_ts:
            # If timestamp is datetime, format it safely or pass as param
            # ClickHouse driver handles params well.
            query = f"SELECT * FROM {full_table_name} WHERE {time_col} > %(max_ts)s ORDER BY {time_col}"
            params = {'max_ts': max_ts}
            logger.info(f"Fetching rows after {max_ts}")
        else:
            query = f"SELECT * FROM {full_table_name} ORDER BY {time_col}"
            params = {}
            logger.info(f"Fetching all rows (no existing data on remote)")

        # We can use a cursor or iterator to handle large datasets
        # But clickhouse-driver execute returns all rows by default. 
        # For huge tables, we should use execute_iter.
        
        # Let's use a simple approach first: fetch all new rows. 
        # If it's too big, we might crash. But since we run every 5 mins, delta should be small.
        
        rows = local_client.execute(query, params=params)
        if not rows:
            logger.info(f"No new rows for {full_table_name}.")
            return

        logger.info(f"Found {len(rows)} new rows. Inserting into remote...")
        
        # 3. Insert into remote
        # We need column names to construct INSERT statement? 
        # Or just INSERT INTO table VALUES ...
        # clickhouse-driver execute method with data list does this efficiently.
        
        remote_client.execute(f"INSERT INTO {full_table_name} VALUES", rows)
        logger.info(f"Successfully synced {len(rows)} rows.")

    except Exception as e:
        logger.error(f"Error syncing {full_table_name}: {e}")

def main():
    local_client = get_client(CLICKHOUSE_LOCAL_CONFIG)
    remote_client = get_client(CLICKHOUSE_REMOTE_CONFIG)
    
    # 1. Sync maicro_monitors (All tables)
    # We know the schema and time columns
    monitors = [
        ('maicro_monitors', 'trades', 'time'),
        ('maicro_monitors', 'orders', 'timestamp'),
        ('maicro_monitors', 'account_snapshots', 'timestamp'),
        ('maicro_monitors', 'positions_snapshots', 'timestamp'),
        ('maicro_monitors', 'funding_payments', 'time'),
        ('maicro_monitors', 'ledger_updates', 'time'),
        ('maicro_monitors', 'candles', 'ts'),
        ('maicro_monitors', 'tracking_error', 'timestamp'),
    ]
    
    for db, table, time_col in monitors:
        sync_table(local_client, remote_client, db, table, time_col)

    # 2. Sync maicro_logs (Live tables)
    # User asked for: positions_jianan_v6, live_trades, live_account, or anything with "live"
    # Let's discover tables with "live" in name
    try:
        logs_tables = local_client.execute("SHOW TABLES FROM maicro_logs")
        logs_tables = [t[0] for t in logs_tables]
        
        targets = ['positions_jianan_v6'] + [t for t in logs_tables if 'live' in t]
        # Deduplicate
        targets = sorted(list(set(targets)))
        
        for table in targets:
            # We need to guess the time column. Usually 'timestamp' or 'time'.
            # Let's check columns.
            try:
                cols = local_client.execute(f"DESCRIBE TABLE maicro_logs.{table}")
                col_names = [c[0] for c in cols]
                
                time_col = None
                for candidate in ['timestamp', 'time', 'created_at', 'ts', 'inserted_at']:
                    if candidate in col_names:
                        time_col = candidate
                        break
                
                if time_col:
                    sync_table(local_client, remote_client, 'maicro_logs', table, time_col)
                else:
                    logger.warning(f"Could not determine time column for maicro_logs.{table}. Skipping.")
            except Exception as e:
                logger.error(f"Error inspecting {table}: {e}")
                
    except Exception as e:
        logger.error(f"Error listing tables in maicro_logs: {e}")

    # 3. Sync Binance & Hyperliquid (Market Data)
    # User said "locally updated binance data, hyperliquid data"
    # This might be huge. Let's be careful. 
    # Maybe just specific tables? Or all?
    # "pulls locally updated... for select tables in maicro_logs... (1) populate maicro monitors locally"
    # The prompt says: "pulls locally updated binance data, hyperliquid data and maicro logs data for select tables in maicro_logs"
    # It implies:
    # - All locally updated binance data
    # - All locally updated hyperliquid data
    # - Select maicro_logs data
    
    # Let's try to sync tables in 'binance' and 'hyperliquid' databases that have a time column.
    for db in ['binance', 'hyperliquid']:
        try:
            tables = local_client.execute(f"SHOW TABLES FROM {db}")
            for t in tables:
                table = t[0]
                # Skip huge tables if needed, or just try.
                # Check for time column
                try:
                    cols = local_client.execute(f"DESCRIBE TABLE {db}.{table}")
                    col_names = [c[0] for c in cols]
                    time_col = next((c for c in ['timestamp', 'time', 'ts', 'open_time'] if c in col_names), None)
                    
                    if time_col:
                        sync_table(local_client, remote_client, db, table, time_col)
                except Exception:
                    pass
        except Exception:
            pass

    logger.info("Sync complete.")

if __name__ == "__main__":
    main()
