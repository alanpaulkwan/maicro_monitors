#!/usr/bin/env python3
"""
pull_data_downward_from_cloud.py
--------------------------------

Daily replication from ClickHouse Cloud → chenlin04.fbe.hku.hk.

Databases synced (mirrors the old `pull_cloud_to_local.py`):
  - hyperliquid
  - maicro_logs
  - binance

For each table:
  - If missing locally: CREATE TABLE from cloud (engine normalized)
    and do an initial full copy via `remoteSecure`.
  - If present: use a date/timestamp cursor column (override or inferred)
    to INSERT only rows with {date_col} > max_local({date_col}).

Local target: CLICKHOUSE_LOCAL_CONFIG  (chenlin04.fbe.hku.hk, maicrobot)
Remote source: CLICKHOUSE_REMOTE_CONFIG (ClickHouse Cloud).
"""

import sys
import os
from datetime import datetime
from typing import Dict, Tuple, List, Optional

from clickhouse_driver import Client

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from config.settings import CLICKHOUSE_LOCAL_CONFIG, CLICKHOUSE_REMOTE_CONFIG  # noqa: E402


DATABASES_TO_SYNC = ["hyperliquid", "maicro_logs", "binance", "maicro_monitors"]

# Optional per-table cursor overrides: (database, table) -> column name
CURSOR_OVERRIDES: Dict[Tuple[str, str], str] = {
    # --- maicro_monitors ---
    ("maicro_monitors", "account_snapshots"): "timestamp",
    ("maicro_monitors", "positions_snapshots"): "timestamp",
    ("maicro_monitors", "trades"): "time",
    ("maicro_monitors", "orders"): "timestamp",
    ("maicro_monitors", "funding_payments"): "time",
    ("maicro_monitors", "ledger_updates"): "time",
    ("maicro_monitors", "candles"): "updated_at",
    ("maicro_monitors", "hl_meta"): "updated_at",
    ("maicro_monitors", "tracking_error"): "timestamp",

    # --- binance ---
    ("binance", "bn_funding_rates"): "fundingTime",
    ("binance", "bn_margin_interest_rates"): "timestamp",
    ("binance", "bn_option_klines"): "timestamp",
    ("binance", "bn_option_symbols_active"): "expiryDate",
    ("binance", "bn_option_symbols_exercised"): "expiryDate",
    ("binance", "bn_perp_klines"): "timestamp",
    ("binance", "bn_perp_symbols"): "onboard_date",
    ("binance", "bn_premium"): "timestamp",
    ("binance", "bn_spot_klines"): "timestamp",
    # bn_spot_symbols has no Date/DateTime column → no override
    ("binance", "cg_coin_data"): "last_updated",

    # --- hyperliquid ---
    ("hyperliquid", "api_data"): "inserted_at",
    ("hyperliquid", "api_data_v2"): "inserted_at",
    ("hyperliquid", "asset_ctx"): "time",
    # market_data, market_data_update, upload_log have no Date/DateTime column

    # --- maicro_logs ---
    ("maicro_logs", "binance_update"): "timestamp",
    ("maicro_logs", "daily_pnl"): "updated_at",
    ("maicro_logs", "feature_store"): "inserted_at",
    ("maicro_logs", "hl_meta"): "updated_at",
    ("maicro_logs", "hourly_prices"): "updated_at",
    ("maicro_logs", "hyperliquid_account_value_snapshots"): "inserted_at",
    ("maicro_logs", "hyperliquid_all_events"): "inserted_at",
    ("maicro_logs", "hyperliquid_daily_value"): "inserted_at",
    ("maicro_logs", "hyperliquid_hourly_value"): "inserted_at",
    ("maicro_logs", "hyperliquid_position_snapshots"): "inserted_at",
    ("maicro_logs", "live_account"): "ts",
    ("maicro_logs", "live_positions"): "ts",
    ("maicro_logs", "live_trades"): "ts",
    ("maicro_logs", "position_mtm"): "updated_at",
    ("maicro_logs", "positions_jianan"): "inserted_at",
    ("maicro_logs", "positions_jianan_mistake_ignore"): "inserted_at",
    ("maicro_logs", "positions_jianan_v5"): "inserted_at",
    ("maicro_logs", "positions_jianan_v6"): "inserted_at",
    ("maicro_logs", "trade_pnl"): "updated_at",
}

# Tables that exist in maicro_logs on Cloud but are considered deprecated
# for chenlin04 and should NOT be down-synced anymore.
SKIP_TABLES: Tuple[Tuple[str, str], ...] = (
    ("maicro_logs", "positions_jianan"),
    ("maicro_logs", "positions_jianan_mistake_ignore"),
    ("maicro_logs", "positions_jianan_v5"),
)


def get_local_client() -> Client:
    """Connect to local ClickHouse (chenlin04)."""
    return Client(**CLICKHOUSE_LOCAL_CONFIG)


def get_remote_client() -> Client:
    """Connect to ClickHouse Cloud."""
    return Client(**CLICKHOUSE_REMOTE_CONFIG)


def get_tables_in_database(client: Client, database: str) -> List[Tuple[str, str]]:
    q = f"""
    SELECT name, engine
    FROM system.tables
    WHERE database = '{database}'
      AND engine NOT IN ('View', 'MaterializedView')
    ORDER BY name
    """
    result = client.execute(q)
    return [(row[0], row[1]) for row in result]


def get_table_create_statement(client: Client, database: str, table: str) -> str:
    q = f"SHOW CREATE TABLE {database}.{table}"
    result = client.execute(q)
    return result[0][0]


def get_table_columns(client: Client, database: str, table: str) -> List[Tuple[str, str]]:
    q = f"""
    SELECT name, type
    FROM system.columns
    WHERE database = '{database}' AND table = '{table}'
    ORDER BY position
    """
    return client.execute(q)


def local_database_exists(local_client: Client, database: str) -> bool:
    q = f"SELECT count() FROM system.databases WHERE name = '{database}'"
    return local_client.execute(q)[0][0] > 0


def local_table_exists(local_client: Client, database: str, table: str) -> bool:
    q = f"SELECT count() FROM system.tables WHERE database = '{database}' AND name = '{table}'"
    return local_client.execute(q)[0][0] > 0


def remote_column_exists(remote_client: Client, database: str, table: str, column: str) -> bool:
    q = (
        "SELECT count() FROM system.columns "
        f"WHERE database = '{database}' AND table = '{table}' AND name = '{column}'"
    )
    try:
        return remote_client.execute(q)[0][0] > 0
    except Exception:
        return False


def create_database_if_not_exists(local_client: Client, database: str) -> None:
    if not local_database_exists(local_client, database):
        print(f"Creating database: {database}")
        local_client.execute(f"CREATE DATABASE IF NOT EXISTS {database}")


def convert_cloud_create_statement(create_statement: str) -> str:
    """Convert shared engines from Cloud to on-premise MergeTree variants."""
    import re

    create_statement = create_statement.replace("SharedReplacingMergeTree", "ReplacingMergeTree")
    create_statement = create_statement.replace("SharedMergeTree", "MergeTree")
    create_statement = create_statement.replace("SharedAggregatingMergeTree", "AggregatingMergeTree")

    # Normalize old-style MergeTree(engine_params...) → MergeTree()
    create_statement = re.sub(
        r"ENGINE\s*=\s*(ReplacingMergeTree|MergeTree|AggregatingMergeTree)\s*\([^)]*\)",
        r"ENGINE = \1()",
        create_statement,
    )
    return create_statement


def get_local_max_date(local_client: Client, database: str, table: str, date_column: str) -> Tuple[Optional[datetime], Optional[str]]:
    try:
        q = f"SELECT max({date_column}) FROM {database}.{table}"
        result = local_client.execute(q)
        max_date = result[0][0]

        type_q = f"""
        SELECT type FROM system.columns
        WHERE database = '{database}' AND table = '{table}' AND name = '{date_column}'
        """
        col_type = local_client.execute(type_q)[0][0]
        return max_date, col_type
    except Exception as e:
        print(f"  Warning: Could not get max({date_column}) for {database}.{table}: {e}")
        return None, None


def sync_table_initial(local_client: Client, remote_client: Client, database: str, table: str) -> None:
    """Initial full copy for a table that doesn't yet exist locally."""
    print(f"  Initial copy of {database}.{table}...")
    create_statement = get_table_create_statement(remote_client, database, table)
    create_statement = convert_cloud_create_statement(create_statement)
    local_client.execute(create_statement)

    remote = CLICKHOUSE_REMOTE_CONFIG
    insert_query = f"""
    INSERT INTO {database}.{table}
    SELECT * FROM remoteSecure(
        '{remote['host']}:9440',
        {database},
        {table},
        '{remote['user']}',
        '{remote['password']}'
    )
    """
    try:
        local_client.execute(insert_query)
        print("  ✓ Initial copy completed")
    except Exception as e:
        print(f"  ✗ Error during initial copy: {e}")
        raise


def find_date_column(remote_client: Client, database: str, table: str) -> Optional[str]:
    q = f"""
    SELECT name, type
    FROM system.columns
    WHERE database = '{database}' AND table = '{table}'
      AND (type LIKE '%Date%' OR type LIKE '%Time%')
    ORDER BY
        CASE
            WHEN name LIKE '%timestamp%' THEN 1
            WHEN name LIKE '%time%' THEN 2
            WHEN name LIKE '%date%' THEN 3
            ELSE 4
        END,
        name
    LIMIT 1
    """
    result = remote_client.execute(q)
    if result:
        return result[0][0]
    return None


def sync_table_incremental(local_client: Client, remote_client: Client, database: str, table: str, date_column: str) -> None:
    max_date, col_type = get_local_max_date(local_client, database, table, date_column)
    if max_date is None:
        print("  No local max date; skipping incremental sync")
        return

    # Format according to column type
    if "Date" in col_type and "DateTime" not in col_type:
        max_str = max_date.strftime("%Y-%m-%d") if hasattr(max_date, "strftime") else str(max_date)
    else:
        if hasattr(max_date, "replace") and hasattr(max_date, "tzinfo"):
            max_date = max_date.replace(tzinfo=None)
        max_str = max_date.strftime("%Y-%m-%d %H:%M:%S") if hasattr(max_date, "strftime") else str(max_date).split("+")[0]

    print(f"  Incremental sync from {date_column} > {max_str}...")
    remote = CLICKHOUSE_REMOTE_CONFIG
    insert_query = f"""
    INSERT INTO {database}.{table}
    SELECT * FROM remoteSecure(
        '{remote['host']}:9440',
        {database},
        {table},
        '{remote['user']}',
        '{remote['password']}'
    )
    WHERE {date_column} > '{max_str}'
    """
    try:
        local_client.execute(insert_query)
        count_q = f"SELECT count() FROM {database}.{table} WHERE {date_column} > '{max_str}'"
        count = local_client.execute(count_q)[0][0]
        print(f"  ✓ Synced {count} new rows")
    except Exception as e:
        print(f"  ✗ Error during incremental sync: {e}")


def sync_database(local_client: Client, remote_client: Client, database: str) -> None:
    print("\n" + "=" * 60)
    print(f"Syncing database: {database}")
    print("=" * 60)

    create_database_if_not_exists(local_client, database)
    try:
        tables = get_tables_in_database(remote_client, database)
    except Exception as e:
        print(f"Error listing tables in remote database {database}: {e}")
        return

    if not tables:
        print(f"No tables found in {database}")
        return

    print(f"Found {len(tables)} tables to sync.")

    for table_name, engine in tables:
        if (database, table_name) in SKIP_TABLES:
            print(f"\n--- Table: {database}.{table_name} (skipped - deprecated) ---")
            continue
        print(f"\n--- Table: {database}.{table_name} (Engine: {engine}) ---")
        try:
            if not local_table_exists(local_client, database, table_name):
                sync_table_initial(local_client, remote_client, database, table_name)
            else:
                print("  Table exists locally.")

            # Determine cursor column
            override = CURSOR_OVERRIDES.get((database, table_name))
            date_column = None
            if override and remote_column_exists(remote_client, database, table_name, override):
                date_column = override
                print(f"  Cursor override: using {date_column}")
            else:
                inferred = find_date_column(remote_client, database, table_name)
                if inferred:
                    date_column = inferred
                    print(f"  Inferred cursor column: {date_column}")

            if date_column:
                sync_table_incremental(local_client, remote_client, database, table_name, date_column)
            else:
                print("  No suitable cursor column; skipping incremental sync.")
        except Exception as e:
            print(f"  ✗ Error syncing table {database}.{table_name}: {e}")


def main():
    print(f"[pull_data_downward_from_cloud] Starting at {datetime.utcnow()}")
    local_client = get_local_client()
    remote_client = get_remote_client()

    for db in DATABASES_TO_SYNC:
        sync_database(local_client, remote_client, db)

    print(f"[pull_data_downward_from_cloud] Done at {datetime.utcnow()}")


if __name__ == "__main__":
    main()
