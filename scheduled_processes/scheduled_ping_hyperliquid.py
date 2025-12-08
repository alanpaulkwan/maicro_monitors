#!/usr/bin/env python3
"""
scheduled_ping_hyperliquid.py
-----------------------------

Hourly buffer-only ping:
  - Calls Hyperliquid HTTP APIs (user state, fills, orders, funding,
    non-funding ledger, OHLCV candles)
  - Writes results to `data/buffer/*.parquet` using BufferManager
  - Does NOT talk to ClickHouse (no insert_df, no query_df).
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pandas as pd

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from config.settings import HYPERLIQUID_ADDRESS  # type: ignore  # noqa: E402
from modules.hyperliquid_client import HyperliquidClient  # noqa: E402
from modules.buffer_manager import BufferManager  # noqa: E402


def _now() -> datetime:
    return datetime.utcnow()


def sync_account_and_positions(hl: HyperliquidClient, buffer_mgr: BufferManager) -> None:
    print("[account/positions] Fetching user state...")
    try:
        state_bundle = hl.fetch_account_state()
    except Exception as e:
        print(f"[account/positions] Error fetching state: {e}")
        return

    state = state_bundle.get("raw")
    if not state:
        print("[account/positions] No user state returned in raw bundle.")
        return

    ts = _now()
    print(f"[account/positions] Fetched state. Equity: {state_bundle['equity_usd']:.2f}, Margin Used: {state_bundle['margin_used_usd']}")

    # Account snapshot
    # We use the robustly parsed values for equity and margin usage,
    # and fall back to raw extraction for fields not covered by fetch_account_state.
    margin_summary = state.get("marginSummary", {}) or {}
    cross_margin_summary = state.get("crossMarginSummary", {}) or {}

    account_row = {
        "timestamp": ts,
        "accountValue": state_bundle["equity_usd"],  # Robustly parsed equity
        "totalMarginUsed": state_bundle["margin_used_usd"] if state_bundle["margin_used_usd"] is not None else float(margin_summary.get("totalMarginUsed", 0.0)),
        "totalNtlPos": float(margin_summary.get("totalNtlPos", 0.0)),
        "totalRawUsd": float(margin_summary.get("totalRawUsd", 0.0)),
        "marginUsed": float(cross_margin_summary.get("marginUsed", 0.0)),
        "withdrawable": float(state.get("withdrawable", 0.0)),
    }
    df_account = pd.DataFrame([account_row])
    buffer_mgr.save(df_account, "account")
    print(f"[account] Buffered 1 account snapshot at {ts}.")

    # Positions snapshot – only non‑zero positions
    # fetch_account_state provides a simplified "positions" dict (symbol->size),
    # but we need full details, so we iterate the raw assetPositions.
    positions = state.get("assetPositions", []) or []
    pos_rows: List[Dict[str, Any]] = []
    for p in positions:
        pos = p.get("position") or {}
        szi = float(pos.get("szi", 0.0) or 0.0)
        if szi == 0:
            continue
        row = {
            "timestamp": ts,
            "coin": pos.get("coin", ""),
            "szi": szi,
            "entryPx": float(pos.get("entryPx", 0.0) or 0.0),
            "positionValue": float(pos.get("positionValue", 0.0) or 0.0),
            "unrealizedPnl": float(pos.get("unrealizedPnl", 0.0) or 0.0),
            "returnOnEquity": float(pos.get("returnOnEquity", 0.0) or 0.0),
            "liquidationPx": float(pos.get("liquidationPx", 0.0) or 0.0),
            "leverage": float((pos.get("leverage") or {}).get("value", 0.0)),
            "maxLeverage": int(pos.get("maxLeverage", 0) or 0),
            "marginUsed": float(pos.get("marginUsed", 0.0) or 0.0),
        }
        pos_rows.append(row)

    if pos_rows:
        df_pos = pd.DataFrame(pos_rows)
        buffer_mgr.save(df_pos, "positions")
        print(f"[positions] Buffered {len(df_pos)} position snapshots at {ts}.")
    else:
        print("[positions] No active positions.")


def sync_trades(hl: HyperliquidClient, buffer_mgr: BufferManager) -> None:
    print("[trades] Fetching user fills...")
    fills = hl.get_user_fills() or []
    if not fills:
        print("[trades] No fills returned.")
        return

    df = pd.DataFrame(fills)
    # Expected fields: coin, side, px, sz, time, hash, startPosition, dir, closedPnl, oid, cloid, fee, tid
    df["time"] = pd.to_datetime(df["time"], unit="ms")

    for col in ["px", "sz", "fee", "closedPnl", "startPosition"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    if "tid" not in df.columns:
        # Fallback ID: millis since epoch
        df["tid"] = (df["time"].astype("int64") // 10**6).astype("int64")

    cols = [
        "coin",
        "side",
        "px",
        "sz",
        "time",
        "hash",
        "startPosition",
        "dir",
        "closedPnl",
        "oid",
        "cloid",
        "fee",
        "tid",
    ]
    df = df[[c for c in cols if c in df.columns]]
    buffer_mgr.save(df, "trades")
    print(f"[trades] Buffered {len(df)} fills.")


def sync_orders(hl: HyperliquidClient, buffer_mgr: BufferManager) -> None:
    print("[orders] Fetching historical orders...")
    orders = hl.get_historical_orders() or []
    if not orders:
        print("[orders] No historical orders returned.")
        return

    flat_orders: List[Dict[str, Any]] = []
    for o in orders:
        order = (o.get("order") or {}).copy()
        order["status"] = o.get("status", "")
        flat_orders.append(order)

    df = pd.DataFrame(flat_orders)
    if df.empty:
        print("[orders] Empty DataFrame after flattening.")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    for col in ["limitPx", "sz"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    cols = ["coin", "side", "limitPx", "sz", "oid", "timestamp", "status", "orderType", "reduceOnly"]
    df = df[[c for c in cols if c in df.columns]]
    buffer_mgr.save(df, "orders")
    print(f"[orders] Buffered {len(df)} orders.")


def sync_funding(hl: HyperliquidClient, buffer_mgr: BufferManager) -> None:
    print("[funding] Fetching funding (last 30 days)...")
    start_time_ms = int((_now() - timedelta(days=30)).timestamp() * 1000)
    funding = hl.get_user_funding(start_time=start_time_ms) or []
    if not funding:
        print("[funding] No funding payments found.")
        return

    rows: List[Dict[str, Any]] = []
    for item in funding:
        delta = item.get("delta", {}) or {}
        rows.append(
            {
                "time": item["time"],
                "coin": delta.get("coin", ""),
                "usdc": delta.get("usdc", 0.0),
                "szi": delta.get("szi", 0.0),
                "fundingRate": delta.get("fundingRate", 0.0),
            }
        )

    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    df["usdc"] = df["usdc"].astype(float)
    df["szi"] = df["szi"].astype(float)
    df["fundingRate"] = df["fundingRate"].astype(float)
    df["tid"] = df["time"].astype("int64") // 10**6

    cols = ["time", "coin", "usdc", "szi", "fundingRate", "tid"]
    df = df[cols]
    buffer_mgr.save(df, "funding")
    print(f"[funding] Buffered {len(df)} funding rows.")


def sync_ledger(hl: HyperliquidClient, buffer_mgr: BufferManager) -> None:
    print("[ledger] Fetching non‑funding ledger updates (last 30 days)...")
    start_time_ms = int((_now() - timedelta(days=30)).timestamp() * 1000)
    updates = hl.get_user_non_funding_ledger_updates(start_time=start_time_ms) or []
    if not updates:
        print("[ledger] No ledger updates found.")
        return

    import json

    rows: List[Dict[str, Any]] = []
    for item in updates:
        delta = item.get("delta", {}) or {}
        rows.append(
            {
                "time": item["time"],
                "hash": item.get("hash", ""),
                "type": delta.get("type", "unknown"),
                "usdc": float(delta.get("usdc", 0.0) or 0.0),
                "coin": delta.get("coin", ""),
                "raw_json": json.dumps(delta),
            }
        )

    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    cols = ["time", "hash", "type", "usdc", "coin", "raw_json"]
    df = df[cols]
    buffer_mgr.save(df, "ledger")
    print(f"[ledger] Buffered {len(df)} ledger updates.")


def _discover_target_coins() -> List[str]:
    """Fixed small universe for buffer-only ping (no ClickHouse queries)."""
    return sorted({"BTC", "ETH", "SOL", "HYPE", "XRP", "DOGE"})


def sync_candles(hl: HyperliquidClient, buffer_mgr: BufferManager) -> None:
    print("[candles] Fetching OHLCV candles...")
    coins = _discover_target_coins()
    print(f"[candles] Target coins: {coins}")

    intervals = [("1h", 48), ("1d", 7 * 24)]  # 48 hours of 1h; 7d of 1d
    all_rows: List[Dict[str, Any]] = []

    now_ms = int(_now().timestamp() * 1000)
    for coin in coins:
        for interval, hours_back in intervals:
            start_ms = int((_now() - timedelta(hours=hours_back)).timestamp() * 1000)
            try:
                candles = hl.get_candles(coin, interval, start_ms, now_ms) or []
            except Exception as e:
                print(f"[candles] Error fetching {interval} candles for {coin}: {e}")
                continue

            for c in candles:
                all_rows.append(
                    {
                        "coin": coin,
                        "interval": interval,
                        "ts": c["t"],
                        "open": float(c["o"]),
                        "high": float(c["h"]),
                        "low": float(c["l"]),
                        "close": float(c["c"]),
                        "volume": float(c["v"]),
                    }
                )

    if not all_rows:
        print("[candles] No candles fetched.")
        return

    df = pd.DataFrame(all_rows)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    buffer_mgr.save(df, "candles")
    print(f"[candles] Buffered {len(df)} candle rows.")


def main():
    print(f"[scheduled_ping_hyperliquid] (buffer-only) Starting for {HYPERLIQUID_ADDRESS} at {_now()}")
    hl = HyperliquidClient(HYPERLIQUID_ADDRESS)
    buffer_mgr = BufferManager()

    sync_account_and_positions(hl, buffer_mgr)
    sync_trades(hl, buffer_mgr)
    sync_orders(hl, buffer_mgr)
    sync_funding(hl, buffer_mgr)
    sync_ledger(hl, buffer_mgr)
    sync_candles(hl, buffer_mgr)

    print(f"[scheduled_ping_hyperliquid] (buffer-only) Done at {_now()}")


if __name__ == "__main__":
    main()
