#!/usr/bin/env python3
"""
flush_hyperliquid_buffers.py
----------------------------

Flushes buffered Hyperliquid data from `data/buffer/*.parquet` into
two ClickHouse targets:

1. Local/chenlin host (CLICKHOUSE_LOCAL_CONFIG)
2. ClickHouse Cloud (CLICKHOUSE_REMOTE_CONFIG)

This script should be run less frequently (e.g. every 3 hours) to avoid
waking ClickHouse too often. It assumes that `scheduled_ping_hyperliquid.py`
has been buffering data via BufferManager.save() using the prefixes:

  - account   -> maicro_monitors.account_snapshots
  - positions -> maicro_monitors.positions_snapshots
  - trades    -> maicro_monitors.trades
  - orders    -> maicro_monitors.orders
  - funding   -> maicro_monitors.funding_payments
  - ledger    -> maicro_monitors.ledger_updates
  - candles   -> maicro_monitors.candles
"""

import os
import glob
from typing import List, Tuple

import pandas as pd
from clickhouse_driver import Client

import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from config.settings import CLICKHOUSE_LOCAL_CONFIG, CLICKHOUSE_REMOTE_CONFIG  # noqa: E402


BUFFER_DIR = os.path.join(REPO_ROOT, "data", "buffer")

PREFIX_TABLES: List[Tuple[str, str]] = [
    ("account", "maicro_monitors.account_snapshots"),
    ("positions", "maicro_monitors.positions_snapshots"),
    ("trades", "maicro_monitors.trades"),
    ("orders", "maicro_monitors.orders"),
    ("funding", "maicro_monitors.funding_payments"),
    ("ledger", "maicro_monitors.ledger_updates"),
    ("candles", "maicro_monitors.candles"),
    ("meta", "maicro_monitors.hl_meta"),
]

# Prefixes whose target tables use ReplacingMergeTree and should be optimized
REPLACING_MERGE_TREE_PREFIXES = {"trades", "orders", "funding", "ledger", "candles"}


def get_clients() -> Tuple[Client, Client]:
    local_client = Client(**CLICKHOUSE_LOCAL_CONFIG)
    remote_client = Client(**CLICKHOUSE_REMOTE_CONFIG)
    return local_client, remote_client


def _common_insert_columns(table: str, local_client: Client, remote_client: Client, df: pd.DataFrame) -> List[str]:
    """
    Determine a safe column subset to insert: intersection of
    (local table columns) ∩ (remote table columns) ∩ (df columns),
    preserving local table column order.
    """
    def describe(client: Client) -> List[str]:
        rows = client.execute(f"DESCRIBE TABLE {table}")
        return [r[0] for r in rows]

    local_cols = describe(local_client)
    remote_cols = describe(remote_client)
    df_cols = set(df.columns)

    common = [c for c in local_cols if c in remote_cols and c in df_cols]
    return common


def flush_prefix(prefix: str, table: str, local_client: Client, remote_client: Client) -> None:
    pattern = os.path.join(BUFFER_DIR, f"{prefix}_*.parquet")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"[{prefix}] No buffered files to flush.")
        return

    print(f"[{prefix}] Found {len(files)} buffered files. Loading...")
    dfs = []
    valid_files = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
            valid_files.append(f)
        except Exception as e:
            print(f"[{prefix}] Error reading {f}: {e}")

    if not dfs:
        print(f"[{prefix}] No valid dataframes to flush.")
        return

    df = pd.concat(dfs, ignore_index=True)
    if df.empty:
        print(f"[{prefix}] Combined dataframe is empty; nothing to insert.")
        return

    # Determine common columns that are safe to insert into both targets
    cols = _common_insert_columns(table, local_client, remote_client, df)
    if not cols:
        print(f"[{prefix}] No common columns between df and table {table}; skipping.")
        return

    df = df[cols]
    values = [tuple(row[c] for c in cols) for _, row in df.iterrows()]

    col_list = ", ".join(cols)
    print(f"[{prefix}] Inserting {len(values)} rows into {table} ({col_list}) on both local and remote...")
    try:
        local_client.execute(f"INSERT INTO {table} ({col_list}) VALUES", values)
        remote_client.execute(f"INSERT INTO {table} ({col_list}) VALUES", values)

        if prefix in REPLACING_MERGE_TREE_PREFIXES:
            print(f"[{prefix}] Running OPTIMIZE TABLE FINAL...")
            try:
                local_client.execute(f"OPTIMIZE TABLE {table} FINAL")
                remote_client.execute(f"OPTIMIZE TABLE {table} FINAL")
            except Exception as opt_e:
                print(f"[{prefix}] Warning: OPTIMIZE failed: {opt_e}")

    except Exception as e:
        print(f"[{prefix}] ERROR inserting into ClickHouse: {e}")
        print(f"[{prefix}] Buffer files retained for retry.")
        return

    # Delete files only after successful insert on both targets
    for f in valid_files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"[{prefix}] Error deleting {f}: {e}")

    print(f"[{prefix}] Flush complete; buffer cleared.")


def main():
    print("[flush_hyperliquid_buffers] Starting dual-target flush...")
    if not os.path.isdir(BUFFER_DIR):
        print(f"[flush] Buffer directory does not exist: {BUFFER_DIR}")
        return

    local_client, remote_client = get_clients()

    for prefix, table in PREFIX_TABLES:
        flush_prefix(prefix, table, local_client, remote_client)

    print("[flush_hyperliquid_buffers] Done.")


if __name__ == "__main__":
    main()
