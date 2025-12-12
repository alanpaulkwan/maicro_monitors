#!/usr/bin/env python3
"""
scheduled_ping_hyperliquid.py
-----------------------------

15-minute buffer-only ping:
  - Calls Hyperliquid HTTP APIs for MULTIPLE ACCOUNTS.
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

from config.settings import HYPERLIQUID_ADDRESSES  # type: ignore  # noqa: E402
from modules.hyperliquid_client import HyperliquidClient  # noqa: E402
from modules.buffer_manager import BufferManager  # noqa: E402


def _now() -> datetime:
    return datetime.utcnow()


def sync_account_and_positions(hl: HyperliquidClient, buffer_mgr: BufferManager, address: str) -> None:
    print(f"[account/positions] Fetching user state for {address[:8]}...")
    try:
        state_bundle = hl.fetch_account_state()
    except Exception as e:
        print(f"[account/positions] Error fetching state for {address[:8]}: {e}")
        return

    state = state_bundle.get("raw")
    if not state:
        print(f"[account/positions] No user state returned for {address[:8]}.")
        return

    ts = _now()
    print(f"[account/positions] Fetched state. Equity: {state_bundle['equity_usd']:.2f}")

    # Account snapshot
    margin_summary = state.get("marginSummary", {}) or {}
    cross_margin_summary = state.get("crossMarginSummary", {}) or {}

    account_row = {
        "timestamp": ts,
        "accountValue": state_bundle["equity_usd"],
        "totalMarginUsed": state_bundle["margin_used_usd"] if state_bundle["margin_used_usd"] is not None else float(margin_summary.get("totalMarginUsed", 0.0)),
        "totalNtlPos": float(margin_summary.get("totalNtlPos", 0.0)),
        "totalRawUsd": float(margin_summary.get("totalRawUsd", 0.0)),
        "marginUsed": float(cross_margin_summary.get("marginUsed", 0.0)),
        "withdrawable": float(state.get("withdrawable", 0.0)),
        "address": address,
    }
    df_account = pd.DataFrame([account_row])
    buffer_mgr.save(df_account, "account")
    print(f"[account] Buffered 1 account snapshot for {address[:8]}.")

    # Positions snapshot
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
            "address": address,
        }
        pos_rows.append(row)

    if pos_rows:
        df_pos = pd.DataFrame(pos_rows)
        buffer_mgr.save(df_pos, "positions")
        print(f"[positions] Buffered {len(df_pos)} positions for {address[:8]}.")
    else:
        print(f"[positions] No active positions for {address[:8]}.")


def sync_trades(hl: HyperliquidClient, buffer_mgr: BufferManager, address: str) -> None:
    print(f"[trades] Fetching user fills for {address[:8]}...")
    fills = hl.get_user_fills() or []
    if not fills:
        print(f"[trades] No fills returned for {address[:8]}.")
        return

    df = pd.DataFrame(fills)
    df["time"] = pd.to_datetime(df["time"], unit="ms")

    for col in ["px", "sz", "fee", "closedPnl", "startPosition"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    if "tid" not in df.columns:
        df["tid"] = (df["time"].astype("int64") // 10**6).astype("int64")

    cols = [
        "coin", "side", "px", "sz", "time", "hash", "startPosition",
        "dir", "closedPnl", "oid", "cloid", "fee", "tid"
    ]
    df = df[[c for c in cols if c in df.columns]].copy()
    df["address"] = address
    
    buffer_mgr.save(df, "trades")
    print(f"[trades] Buffered {len(df)} fills for {address[:8]}.")


def sync_orders(hl: HyperliquidClient, buffer_mgr: BufferManager, address: str) -> None:
    print(f"[orders] Fetching historical orders for {address[:8]}...")
    orders = hl.get_historical_orders() or []
    if not orders:
        print(f"[orders] No historical orders for {address[:8]}.")
        return

    flat_orders: List[Dict[str, Any]] = []
    for o in orders:
        order = (o.get("order") or {}).copy()
        order["status"] = o.get("status", "")
        flat_orders.append(order)

    df = pd.DataFrame(flat_orders)
    if df.empty:
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    for col in ["limitPx", "sz"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    cols = ["coin", "side", "limitPx", "sz", "oid", "timestamp", "status", "orderType", "reduceOnly"]
    df = df[[c for c in cols if c in df.columns]].copy()
    df["address"] = address
    
    buffer_mgr.save(df, "orders")
    print(f"[orders] Buffered {len(df)} orders for {address[:8]}.")


def sync_funding(hl: HyperliquidClient, buffer_mgr: BufferManager, address: str) -> None:
    print(f"[funding] Fetching funding for {address[:8]}...")
    start_time_ms = int((_now() - timedelta(days=30)).timestamp() * 1000)
    funding = hl.get_user_funding(start_time=start_time_ms) or []
    if not funding:
        print(f"[funding] No funding payments for {address[:8]}.")
        return

    rows: List[Dict[str, Any]] = []
    for item in funding:
        delta = item.get("delta", {}) or {}
        rows.append({
            "time": item["time"],
            "coin": delta.get("coin", ""),
            "usdc": delta.get("usdc", 0.0),
            "szi": delta.get("szi", 0.0),
            "fundingRate": delta.get("fundingRate", 0.0),
        })

    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    df["usdc"] = df["usdc"].astype(float)
    df["szi"] = df["szi"].astype(float)
    df["fundingRate"] = df["fundingRate"].astype(float)
    df["tid"] = df["time"].astype("int64") // 10**6
    df["address"] = address

    buffer_mgr.save(df, "funding")
    print(f"[funding] Buffered {len(df)} rows for {address[:8]}.")


def sync_ledger(hl: HyperliquidClient, buffer_mgr: BufferManager, address: str) -> None:
    print(f"[ledger] Fetching ledger updates for {address[:8]}...")
    start_time_ms = int((_now() - timedelta(days=30)).timestamp() * 1000)
    updates = hl.get_user_non_funding_ledger_updates(start_time=start_time_ms) or []
    if not updates:
        print(f"[ledger] No ledger updates for {address[:8]}.")
        return

    import json
    rows: List[Dict[str, Any]] = []
    for item in updates:
        delta = item.get("delta", {}) or {}
        rows.append({
            "time": item["time"],
            "hash": item.get("hash", ""),
            "type": delta.get("type", "unknown"),
            "usdc": float(delta.get("usdc", 0.0) or 0.0),
            "coin": delta.get("coin", ""),
            "raw_json": json.dumps(delta),
        })

    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    df["address"] = address
    
    buffer_mgr.save(df, "ledger")
    print(f"[ledger] Buffered {len(df)} updates for {address[:8]}.")


def _discover_target_coins() -> List[str]:
    """Fixed small universe."""
    return sorted({"BTC", "ETH", "SOL", "HYPE", "XRP", "DOGE"})


def sync_candles(hl: HyperliquidClient, buffer_mgr: BufferManager) -> None:
    """Global market data (not per-account)."""
    print("[candles] Fetching OHLCV candles...")
    coins = _discover_target_coins()
    intervals = [("1h", 48), ("1d", 7 * 24)]
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
                all_rows.append({
                    "coin": coin,
                    "interval": interval,
                    "ts": c["t"],
                    "open": float(c["o"]),
                    "high": float(c["h"]),
                    "low": float(c["l"]),
                    "close": float(c["c"]),
                    "volume": float(c["v"]),
                })

    if all_rows:
        df = pd.DataFrame(all_rows)
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")
        buffer_mgr.save(df, "candles")
        print(f"[candles] Buffered {len(df)} candle rows.")


def sync_meta(hl: HyperliquidClient, buffer_mgr: BufferManager) -> None:
    """Global metadata (not per-account)."""
    print("[meta] Fetching exchange metadata...")
    try:
        meta_info = hl.get_meta_info() or {}
        universe = meta_info.get("universe", [])
    except Exception as e:
        print(f"[meta] Error fetching metadata: {e}")
        return

    rows: List[Dict[str, Any]] = []
    ts = _now()
    min_notional_env = float(os.getenv("MIN_NOTIONAL_USD", "10"))

    for item in universe:
        if not isinstance(item, dict): continue
        symbol = item.get("name")
        if not symbol: continue
            
        sz_decimals = int(item.get("szDecimals", 0))
        px_decimals = max(0, 6 - sz_decimals)
        size_step = 10.0 ** (-sz_decimals)
        tick_size = 10.0 ** (-px_decimals)

        min_units = 0.0
        for k in ("minSz", "minSize", "minUnit"):
            if k in item and item[k] is not None:
                try:
                    min_units = float(item[k])
                    break
                except (ValueError, TypeError):
                    continue

        rows.append({
            "symbol": str(symbol).upper(),
            "sz_decimals": sz_decimals,
            "px_decimals": px_decimals,
            "size_step": size_step,
            "tick_size": tick_size,
            "min_units": min_units,
            "min_usd": min_notional_env,
            "updated_at": ts
        })

    if rows:
        df = pd.DataFrame(rows)
        buffer_mgr.save(df, "meta")
        print(f"[meta] Buffered metadata for {len(df)} symbols.")


def main():
    print(f"[scheduled_ping_hyperliquid] Starting at {_now()}")
    
    if not HYPERLIQUID_ADDRESSES:
        print("No addresses configured in HYPERLIQUID_ADDRESSES.")
        return

    # 1. Global Syncs (using the first address as client, doesn't matter which)
    print("[main] Running global syncs (candles, meta)...")
    hl_global = HyperliquidClient(HYPERLIQUID_ADDRESSES[0])
    buffer_mgr = BufferManager()
    
    sync_candles(hl_global, buffer_mgr)
    sync_meta(hl_global, buffer_mgr)

    # 2. Per-Account Syncs
    for address in HYPERLIQUID_ADDRESSES:
        print(f"\n[main] Processing account: {address}...")
        hl = HyperliquidClient(address)
        
        sync_account_and_positions(hl, buffer_mgr, address)
        sync_trades(hl, buffer_mgr, address)
        sync_orders(hl, buffer_mgr, address)
        sync_funding(hl, buffer_mgr, address)
        sync_ledger(hl, buffer_mgr, address)

    print(f"\n[scheduled_ping_hyperliquid] Done at {_now()}")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
