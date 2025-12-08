#!/usr/bin/env python3
"""
Backfill Hyperliquid snapshots from old maicro_hl_records → maicro_monitors.*
------------------------------------------------------------------------------

Context:
  - The legacy poller `claude/maicro/live_scripts/poll_hyperliquid_once.py`
    has been writing account/position snapshots into:
      * maicro_hl_records.hyperliquid_account_value_snapshots
      * maicro_hl_records.hyperliquid_position_snapshots
    on the local ClickHouse (localhost, user=claude).

  - The new pipeline uses `maicro_monitors.account_snapshots` and
    `maicro_monitors.positions_snapshots` (on chenlin + Cloud).

This script:
  1. Connects to the local "old" ClickHouse on localhost (claude).
  2. Connects to chenlin (CLICKHOUSE_LOCAL_CONFIG).
  3. Connects to ClickHouse Cloud (CLICKHOUSE_REMOTE_CONFIG).
  4. Reads the full history from the maicro_hl_records tables.
  5. Maps to the maicro_monitors.* schemas and inserts into BOTH chenlin
     and Cloud.

Run this once to copy historical account/position snapshots into the
new maicro_monitors tables before turning off the old poller cron.
"""

import os
import sys
from typing import Tuple

import pandas as pd
from clickhouse_driver import Client

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from config.settings import (  # noqa: E402
    CLICKHOUSE_LOCAL_CONFIG,
    CLICKHOUSE_REMOTE_CONFIG,
    get_secret,
)


def get_old_client() -> Client:
    """Connect to the legacy local ClickHouse used by claude/maicro."""
    return Client(
        host=os.getenv("CLICKHOUSE_LEGACY_HOST", "localhost"),
        user=os.getenv("CLICKHOUSE_LEGACY_USER", "claude"),
        password=get_secret("CLICKHOUSE_LEGACY_PASSWORD", ""),
    )


def get_new_clients() -> Tuple[Client, Client]:
    """Connect to chenlin (local) and ClickHouse Cloud (remote)."""
    local_client = Client(**CLICKHOUSE_LOCAL_CONFIG)
    remote_client = Client(**CLICKHOUSE_REMOTE_CONFIG)
    return local_client, remote_client


def backfill_account_snapshots(old: Client, new_local: Client, new_remote: Client) -> None:
    print("[account] Loading from maicro_hl_records.hyperliquid_account_value_snapshots...")
    rows = old.execute(
        """
        SELECT
            snapshot_time,
            account_value,
            total_ntl_pos,
            total_raw_usd,
            total_margin_used,
            withdrawable
        FROM maicro_hl_records.hyperliquid_account_value_snapshots
        ORDER BY snapshot_time
        """
    )
    if not rows:
        print("[account] No rows found; skipping.")
        return

    df = pd.DataFrame(
        rows,
        columns=[
            "timestamp",
            "accountValue",
            "totalNtlPos",
            "totalRawUsd",
            "totalMarginUsed",
            "withdrawable",
        ],
    )
    # marginUsed is not present in the legacy table; approximate as totalMarginUsed
    df["marginUsed"] = df["totalMarginUsed"]

    cols = [
        "timestamp",
        "accountValue",
        "totalMarginUsed",
        "totalNtlPos",
        "totalRawUsd",
        "marginUsed",
        "withdrawable",
    ]
    df = df[cols]
    print(f"[account] Backfilling {len(df)} rows into maicro_monitors.account_snapshots (local + cloud)...")

    new_local.execute(
        "INSERT INTO maicro_monitors.account_snapshots (timestamp, accountValue, totalMarginUsed, totalNtlPos, totalRawUsd, marginUsed, withdrawable) VALUES",
        [tuple(row[c] for c in cols) for _, row in df.iterrows()],
    )
    new_remote.execute(
        "INSERT INTO maicro_monitors.account_snapshots (timestamp, accountValue, totalMarginUsed, totalNtlPos, totalRawUsd, marginUsed, withdrawable) VALUES",
        [tuple(row[c] for c in cols) for _, row in df.iterrows()],
    )
    print("[account] Backfill complete.")


def backfill_position_snapshots(old: Client, new_local: Client, new_remote: Client) -> None:
    print("[positions] Loading from maicro_hl_records.hyperliquid_position_snapshots...")
    rows = old.execute(
        """
        SELECT
            snapshot_time,
            coin,
            size,
            entry_px,
            position_value,
            unrealized_pnl,
            return_on_equity,
            liquidation_px,
            margin_used
        FROM maicro_hl_records.hyperliquid_position_snapshots
        ORDER BY snapshot_time
        """
    )
    if not rows:
        print("[positions] No rows found; skipping.")
        return

    df = pd.DataFrame(
        rows,
        columns=[
            "timestamp",
            "coin",
            "szi",
            "entryPx",
            "positionValue",
            "unrealizedPnl",
            "returnOnEquity",
            "liquidationPx",
            "marginUsed",
        ],
    )
    # maicro_monitors.positions_snapshots has leverage / maxLeverage columns; we don't have
    # that historically from the legacy table, so set them to 0.
    df["leverage"] = 0.0
    df["maxLeverage"] = 0

    cols = [
        "timestamp",
        "coin",
        "szi",
        "entryPx",
        "positionValue",
        "unrealizedPnl",
        "returnOnEquity",
        "liquidationPx",
        "leverage",
        "maxLeverage",
        "marginUsed",
    ]
    df = df[cols]
    print(f"[positions] Backfilling {len(df)} rows into maicro_monitors.positions_snapshots (local + cloud)...")

    new_local.execute(
        "INSERT INTO maicro_monitors.positions_snapshots (timestamp, coin, szi, entryPx, positionValue, unrealizedPnl, returnOnEquity, liquidationPx, leverage, maxLeverage, marginUsed) VALUES",
        [tuple(row[c] for c in cols) for _, row in df.iterrows()],
    )
    new_remote.execute(
        "INSERT INTO maicro_monitors.positions_snapshots (timestamp, coin, szi, entryPx, positionValue, unrealizedPnl, returnOnEquity, liquidationPx, leverage, maxLeverage, marginUsed) VALUES",
        [tuple(row[c] for c in cols) for _, row in df.iterrows()],
    )
    print("[positions] Backfill complete.")


def main() -> None:
    print("[backfill_hl_snapshots_from_maicro] Starting...")
    old = get_old_client()
    new_local, new_remote = get_new_clients()

    backfill_account_snapshots(old, new_local, new_remote)
    backfill_position_snapshots(old, new_local, new_remote)

    print("[backfill_hl_snapshots_from_maicro] Done.")


if __name__ == "__main__":
    main()
