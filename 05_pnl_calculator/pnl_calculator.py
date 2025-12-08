#!/usr/bin/env python3
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.clickhouse_client import query_df

# Assuming these are available from maicro_monitors.candles or similar
# def load_prices(lookback_days): ...

def load_trades(lookback_days=30):
    """Load trade data from maicro_monitors.trades"""
    query = f"""
    SELECT time, coin, side, sz as quantity, px as price, fee, closedPnl
    FROM maicro_monitors.trades
    WHERE time >= now() - INTERVAL {lookback_days} DAY
    ORDER BY time
    """
    df = query_df(query)
    df['time'] = pd.to_datetime(df['time'])
    return df

def load_funding_payments(lookback_days=30):
    """Load funding payment data from maicro_monitors.funding_payments"""
    query = f"""
    SELECT time, coin, usdc
    FROM maicro_monitors.funding_payments
    WHERE time >= now() - INTERVAL {lookback_days} DAY
    ORDER BY time
    """
    df = query_df(query)
    df['time'] = pd.to_datetime(df['time'])
    return df

def load_positions(lookback_days=30):
    """Load position snapshots from maicro_monitors.positions_snapshots"""
    query = f"""
    SELECT timestamp, coin, szi as position_size, entryPx as entry_price, liquidationPx as estimated_liq_price, unrealizedPnl as unrealized_pnl, positionValue
    FROM maicro_monitors.positions_snapshots
    WHERE timestamp >= now() - INTERVAL {lookback_days} DAY
    ORDER BY timestamp
    """
    df = query_df(query)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def load_prices(lookback_days=30, interval='1d'):
    """
    Load market close prices from maicro_monitors.candles.
    
    Returns:
        pd.DataFrame: DataFrame with daily close prices, indexed by date, columns as symbols.
    """
    query = f"""
    SELECT toDate(ts) as date, coin, close
    FROM maicro_monitors.candles
    WHERE ts >= toStartOfHour(now() - INTERVAL {lookback_days + 5} DAY)
    AND interval = '{interval}'
    ORDER BY date, coin
    """
    df = query_df(query)
    
    if df.empty:
        return pd.DataFrame()
    
    df['date'] = pd.to_datetime(df['date'])
    df['coin'] = df['coin'].str.upper() # Standardize coins to uppercase
    
    # Pivot to get coins as columns and date as index
    prices_pivot = df.pivot_table(index='date', columns='coin', values='close')
    
    return prices_pivot

def calculate_pnl(trades_df, funding_df, positions_df, prices_df):
    """
    Calculates realized and unrealized PnL, considering funding and fees.
    """
    realized_pnl = 0.0
    unrealized_pnl = 0.0
    total_fees = 0.0
    total_funding = 0.0

    # 1. Calculate Realized PnL from trades
    if not trades_df.empty:
        # Sum closed PnL directly from trades (assuming it's recorded when position is closed)
        realized_pnl = trades_df['closedPnl'].sum()
        total_fees = trades_df['fee'].sum()
    
    # 2. Calculate Funding Payments
    if not funding_df.empty:
        total_funding = funding_df['usdc'].sum()

    # 3. Calculate Unrealized PnL from latest positions
    if not positions_df.empty:
        # Get the latest snapshot for each coin
        # Group by coin and get the row with the latest timestamp
        latest_positions = positions_df.loc[positions_df.groupby('coin')['timestamp'].idxmax()]
        
        # Hyperliquid provides unrealizedPnl directly in position snapshots, so use it
        unrealized_pnl = latest_positions['unrealized_pnl'].sum()
        
        # If we needed to mark-to-market manually using prices_df:
        # for index, row in latest_positions.iterrows():
        #     coin = row['coin'].upper()
        #     if coin in prices_df.columns and not prices_df[coin].empty:
        #         latest_price = prices_df[coin].iloc[-1] # Get the very last known price
        #         # This is a simplification, ideally you'd match by date/timestamp
        #         if not pd.isna(latest_price):
        #             unrealized_pnl += (latest_price - row['entry_price']) * row['position_size']
        
    total_pnl = realized_pnl + unrealized_pnl + total_funding

    return {
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "total_fees": total_fees,
        "total_funding": total_funding,
        "total_pnl": total_pnl
    }

def main():
    """Main PnL calculation pipeline"""
    print("=" * 60)
    print("PnL Calculator")
    print("=" * 60)

    lookback_days = 30 # For data loading
    
    # Load data
    print(f"\nLoading data for the last {lookback_days} days...")
    trades_df = load_trades(lookback_days)
    funding_df = load_funding_payments(lookback_days)
    positions_df = load_positions(lookback_days)
    prices_df = load_prices(lookback_days, interval='1d')
    print("   ✓ Loaded trade, funding, position, and price data.")

    # Calculate PnL
    print("\nCalculating PnL...")
    pnl_results = calculate_pnl(trades_df, funding_df, positions_df, prices_df)

    print("\n----- PnL Summary -----")
    for k, v in pnl_results.items():
        print(f"{k}: {v:.2f}")
    print("-----------------------")
    
    print("\n" + "=" * 60)
    print("PnL Calculation Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
