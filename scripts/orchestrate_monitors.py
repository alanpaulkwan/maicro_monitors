#!/usr/bin/env python3
import sys
import os
import time
import traceback
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.hyperliquid_client import HyperliquidClient
from modules.buffer_manager import BufferManager
from config.settings import HYPERLIQUID_ADDRESS

# Import monitor logic
# We can import the main functions or refactor them. 
# For now, let's import the modules and call their logic if we can refactor them to be callable.
# Or we can just execute them as subprocesses? 
# Subprocesses is safer for isolation, but importing is cleaner for shared resources.
# Given the "buffer" requirement, we need to modify the logic anyway.

# Let's define the logic for each monitor here or import updated versions.
# To avoid rewriting everything in one file, I will update the individual scripts to be importable 
# and accept a BufferManager instance, or just use the shared one.

def run_account_monitor(hl, buffer_mgr):
    print("--- Running Account Monitor ---")
    try:
        state = hl.get_user_state()
        if not state:
            print("No user state returned.")
            return

        # 1. Account Snapshot
        margin_summary = state.get("marginSummary", {})
        cross_margin_summary = state.get("crossMarginSummary", {})
        timestamp = datetime.now()
        
        account_data = {
            "timestamp": timestamp,
            "accountValue": float(margin_summary.get("accountValue", 0)),
            "totalMarginUsed": float(margin_summary.get("totalMarginUsed", 0)),
            "totalNtlPos": float(margin_summary.get("totalNtlPos", 0)),
            "totalRawUsd": float(margin_summary.get("totalRawUsd", 0)),
            "marginUsed": float(cross_margin_summary.get("marginUsed", 0)),
            "withdrawable": float(state.get("withdrawable", 0))
        }
        
        import pandas as pd
        df_account = pd.DataFrame([account_data])
        buffer_mgr.save(df_account, 'account')

        # 2. Positions Snapshot
        positions = state.get("assetPositions", [])
        pos_rows = []
        if positions:
            for p in positions:
                pos = p.get("position", {})
                if not pos: continue
                szi = float(pos.get("szi", 0))
                if szi == 0: continue
                    
                row = {
                    "timestamp": timestamp,
                    "coin": pos.get("coin", ""),
                    "szi": szi,
                    "entryPx": float(pos.get("entryPx", 0)),
                    "positionValue": float(pos.get("positionValue", 0)),
                    "unrealizedPnl": float(pos.get("unrealizedPnl", 0)),
                    "returnOnEquity": float(pos.get("returnOnEquity", 0)),
                    "liquidationPx": float(pos.get("liquidationPx", 0) or 0),
                    "leverage": float(pos.get("leverage", {}).get("value", 0)),
                    "maxLeverage": int(pos.get("maxLeverage", 0)),
                    "marginUsed": float(pos.get("marginUsed", 0))
                }
                pos_rows.append(row)
        
        if pos_rows:
            df_pos = pd.DataFrame(pos_rows)
            buffer_mgr.save(df_pos, 'positions')
        else:
            print("No active positions.")

    except Exception as e:
        print(f"Error in account monitor: {e}")
        traceback.print_exc()

def run_trade_monitor(hl, buffer_mgr):
    print("--- Running Trade Monitor ---")
    try:
        # Fetch recent fills (last 24h or just latest 500?)
        # get_user_fills returns latest fills.
        fills = hl.get_user_fills()
        if not fills:
            print("No fills found.")
            return

        import pandas as pd
        df = pd.DataFrame(fills)
        
        # Ensure types
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        
        # Select only columns present in schema
        cols = ['coin', 'side', 'px', 'sz', 'time', 'hash', 'startPosition', 'dir', 'closedPnl', 'oid', 'cloid', 'fee', 'tid']
        # Filter columns that exist in df
        cols = [c for c in cols if c in df.columns]
        df = df[cols]
        
        # Ensure numeric types
        for col in ['px', 'sz', 'fee', 'closedPnl', 'startPosition']:
            if col in df.columns:
                df[col] = df[col].astype(float)
        
        # Buffer
        buffer_mgr.save(df, 'trades')
        
    except Exception as e:
        print(f"Error in trade monitor: {e}")
        traceback.print_exc()

def run_order_monitor(hl, buffer_mgr):
    print("--- Running Order Monitor ---")
    try:
        orders = hl.get_historical_orders()
        if not orders:
            print("No historical orders found.")
            return

        flattened_orders = []
        for o in orders:
            flat = o['order'].copy()
            flat['status'] = o['status']
            # Ensure timestamp is handled if needed, but schema uses 'timestamp' from order
            flattened_orders.append(flat)
            
        import pandas as pd
        df = pd.DataFrame(flattened_orders)
        
        # Ensure types
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Select only columns present in schema
        cols = ['coin', 'side', 'limitPx', 'sz', 'oid', 'timestamp', 'status', 'orderType', 'reduceOnly']
        # Filter columns that exist in df
        cols = [c for c in cols if c in df.columns]
        df = df[cols]
        
        # Ensure numeric types
        for col in ['limitPx', 'sz']:
            if col in df.columns:
                df[col] = df[col].astype(float)
        
        buffer_mgr.save(df, 'orders')

    except Exception as e:
        print(f"Error in order monitor: {e}")
        traceback.print_exc()

def run_funding_monitor(hl, buffer_mgr):
    print("--- Running Funding Monitor ---")
    try:
        # Fetch last 30 days. 
        # If DB is down, we rely on this window. 
        # If DB is up, ReplacingMergeTree handles dedup.
        start_time = int((time.time() - 30 * 24 * 3600) * 1000)
        
        funding = hl.get_user_funding(start_time=start_time)
        if not funding:
            print("No funding payments found in last 30 days.")
            return

        rows = []
        for item in funding:
            delta = item.get('delta', {})
            rows.append({
                'time': item['time'],
                'coin': delta.get('coin'),
                'usdc': delta.get('usdc'),
                'szi': delta.get('szi'),
                'fundingRate': delta.get('fundingRate')
            })

        import pandas as pd
        df = pd.DataFrame(rows)
        
        # Schema: coin, usdc, szi, fundingRate, time
        # API returns: coin, usdc, szi, fundingRate, time
        
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        df['usdc'] = df['usdc'].astype(float)
        df['szi'] = df['szi'].astype(float)
        df['fundingRate'] = df['fundingRate'].astype(float)
        # Create tid from timestamp (ms)
        df['tid'] = df['time'].astype('int64') // 10**6 
        
        cols = ['time', 'coin', 'usdc', 'szi', 'fundingRate', 'tid']
        df = df[cols]
        
        buffer_mgr.save(df, 'funding')

    except Exception as e:
        print(f"Error in funding monitor: {e}")
        traceback.print_exc()

def run_ohlcv_monitor(hl, buffer_mgr):
    print("--- Running OHLCV Monitor ---")
    try:
        # 1. Determine coins
        # Fallback list
        coins = {"BTC", "ETH", "SOL", "HYPE", "XRP", "DOGE"}
        
        # Try to fetch from DB if available (optional)
        # Since we are in an orchestrator that assumes DB might be down, 
        # we wrap this in a broad try/except or just skip it if we want to be purely offline-capable.
        # But buffer_mgr doesn't expose query capability. 
        # Let's import query_df locally if we want to try.
        try:
            from modules.clickhouse_client import query_df
            queries = [
                "SELECT DISTINCT coin FROM maicro_monitors.trades",
                "SELECT DISTINCT coin FROM maicro_monitors.orders"
            ]
            for q in queries:
                try:
                    df_c = query_df(q)
                    if not df_c.empty and 'coin' in df_c.columns:
                        found = df_c['coin'].dropna().unique()
                        coins.update(found)
                except Exception:
                    pass # DB likely down
        except ImportError:
            pass

        target_coins = sorted(list(coins))
        print(f"Target coins: {target_coins}")

        # 2. Fetch Candles
        # We'll fetch 1h (last 48h) and 1d (last 7d)
        intervals = [
            ("1h", 48), # 48 hours
            ("1d", 7 * 24) # 7 days
        ]
        
        all_candles = []
        
        for coin in target_coins:
            for interval, hours_back in intervals:
                try:
                    end_time = int(time.time() * 1000)
                    start_time = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
                    
                    candles = hl.get_candles(coin, interval, start_time, end_time)
                    if not candles:
                        continue
                        
                    # API returns: t, T, s, i, o, c, h, l, v, n
                    # We need: coin, interval, ts, open, high, low, close, volume
                    
                    for c in candles:
                        row = {
                            "coin": coin,
                            "interval": interval,
                            "ts": c['t'], # ms timestamp
                            "open": float(c['o']),
                            "high": float(c['h']),
                            "low": float(c['l']),
                            "close": float(c['c']),
                            "volume": float(c['v'])
                        }
                        all_candles.append(row)
                        
                except Exception as e:
                    print(f"Error fetching {interval} candles for {coin}: {e}")
                    continue
        
        if not all_candles:
            print("No candles fetched.")
            return

        import pandas as pd
        df = pd.DataFrame(all_candles)
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        
        buffer_mgr.save(df, 'candles')

    except Exception as e:
        print(f"Error in OHLCV monitor: {e}")
        traceback.print_exc()

def run_ledger_monitor(hl, buffer_mgr):
    print("--- Running Ledger Monitor ---")
    try:
        # Fetch last 30 days
        start_time = int((time.time() - 30 * 24 * 3600) * 1000)
        
        updates = hl.get_user_non_funding_ledger_updates(start_time=start_time)
        if not updates:
            print("No ledger updates found in last 30 days.")
            return

        import json
        rows = []
        for item in updates:
            delta = item.get('delta', {})
            row = {
                'time': item['time'],
                'hash': item['hash'],
                'type': delta.get('type', 'unknown'),
                'usdc': float(delta.get('usdc', 0)),
                'coin': delta.get('coin', ''),
                'raw_json': json.dumps(delta)
            }
            rows.append(row)
            
        import pandas as pd
        df = pd.DataFrame(rows)
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        
        buffer_mgr.save(df, 'ledger')

    except Exception as e:
        print(f"Error in ledger monitor: {e}")
        traceback.print_exc()

def main():
    print(f"Starting Orchestrator for {HYPERLIQUID_ADDRESS} at {datetime.now()}")
    
    hl = HyperliquidClient(HYPERLIQUID_ADDRESS)
    buffer_mgr = BufferManager()
    
    # 1. Run Monitors (Collect Data)
    run_account_monitor(hl, buffer_mgr)
    run_trade_monitor(hl, buffer_mgr)
    run_order_monitor(hl, buffer_mgr)
    run_funding_monitor(hl, buffer_mgr)
    run_ledger_monitor(hl, buffer_mgr)
    run_ohlcv_monitor(hl, buffer_mgr)
    
    # 2. Flush Buffers (Send to DB)
    print("--- Flushing Buffers ---")
    buffer_mgr.flush('account', 'maicro_monitors.account_snapshots')
    buffer_mgr.flush('positions', 'maicro_monitors.positions_snapshots')
    buffer_mgr.flush('trades', 'maicro_monitors.trades')
    buffer_mgr.flush('orders', 'maicro_monitors.orders')
    buffer_mgr.flush('funding', 'maicro_monitors.funding_payments')
    buffer_mgr.flush('ledger', 'maicro_monitors.ledger_updates')
    buffer_mgr.flush('candles', 'maicro_monitors.candles')
    
    print("Orchestration complete.")

if __name__ == "__main__":
    main()
