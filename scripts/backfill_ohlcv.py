#!/usr/bin/env python3
import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import HYPERLIQUID_ADDRESS
from modules.hyperliquid_client import HyperliquidClient
from modules.clickhouse_client import query_df, insert_df

COINS = ["BTC", "ETH", "SOL", "HYPE"] # Fallback list

def get_traded_coins():
    """Get list of coins from various sources."""
    coins = set(COINS)
    
    queries = [
        "SELECT DISTINCT coin FROM maicro_monitors.trades",
        "SELECT DISTINCT coin FROM maicro_monitors.orders",
        "SELECT DISTINCT coin FROM maicro_logs.live_trades",
        "SELECT DISTINCT coin FROM hyperliquid.asset_ctx",
        "SELECT DISTINCT coin FROM hyperliquid.market_data"
    ]
    
    for q in queries:
        try:
            df = query_df(q)
            if not df.empty and 'coin' in df.columns:
                # Ensure we handle potential None/NaN values
                found = df['coin'].dropna().unique()
                coins.update(found)
        except Exception as e:
            print(f"Warning: Could not fetch coins with query '{q}': {e}")
            
    return sorted(list(coins))

def main():
    print("Starting OHLCV BACKFILL...")
    hl = HyperliquidClient(HYPERLIQUID_ADDRESS)
    
    target_coins = get_traded_coins()
    print(f"Target coins: {target_coins}")
    
    end_time = int(time.time() * 1000)
    # Start from Jan 1, 2023
    start_time = int(datetime(2023, 1, 1).timestamp() * 1000)

    for coin in target_coins:
        try:
            print(f"Fetching {coin} from 2023-01-01...")
            candles = hl.get_candles(coin, "1d", start_time, end_time)
            if not candles:
                print(f"No candles for {coin}")
                continue
                
            df = pd.DataFrame(candles)
            # API: t, T, s, i, o, c, h, l, v, n
            # We need: coin, interval, ts, open, high, low, close, volume
            
            df['coin'] = coin
            df['interval'] = '1d'
            df['ts'] = pd.to_datetime(df['t'], unit='ms')
            df['open'] = df['o'].astype(float)
            df['high'] = df['h'].astype(float)
            df['low'] = df['l'].astype(float)
            df['close'] = df['c'].astype(float)
            df['volume'] = df['v'].astype(float)
            df['updated_at'] = datetime.now()
            
            cols = ['coin', 'interval', 'ts', 'open', 'high', 'low', 'close', 'volume', 'updated_at']
            df = df[cols]
            
            insert_df('maicro_monitors.candles', df)
            print(f"Inserted {len(df)} candles for {coin}")
            
        except Exception as e:
            print(f"Error fetching {coin}: {e}")

if __name__ == "__main__":
    main()
