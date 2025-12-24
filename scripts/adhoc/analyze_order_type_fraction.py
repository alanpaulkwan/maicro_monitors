#!/usr/bin/env python3
"""
Calculate order type fraction over time for trades.

This script:
1. Queries trades from maicro_monitors.trades
2. Joins with orders to get orderType
3. Calculates daily fraction of Limit vs Market orders
4. Outputs summary statistics and trends
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Tuple, Optional

import pandas as pd

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from modules.clickhouse_client import query_df


def load_trades_with_order_type(days_back: int = 30) -> pd.DataFrame:
    """Load trades with order type information for the last N days."""
    
    since = datetime.utcnow() - timedelta(days=days_back)
    
    sql = """
    SELECT 
        t.time,
        t.coin,
        t.side,
        t.px,
        t.sz,
        t.oid,
        o.orderType,
        toDate(t.time) as trade_date
    FROM maicro_monitors.trades t
    LEFT JOIN maicro_monitors.orders o ON t.oid = o.oid
    WHERE t.time >= %(since)s
    ORDER BY t.time DESC
    """
    
    df = query_df(sql, params={"since": since})
    
    if df.empty:
        print(f"No trades found in the last {days_back} days")
        return df
    
    # Ensure proper types
    df['time'] = pd.to_datetime(df['time'])
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    df['orderType'] = df['orderType'].fillna('Unknown')
    
    return df


def calculate_daily_fractions(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily order type fractions."""
    
    if df.empty:
        return pd.DataFrame()
    
    # Group by date and order type
    daily_counts = df.groupby(['trade_date', 'orderType']).size().unstack(fill_value=0)
    
    # Ensure all columns exist
    for col in ['Limit', 'Market', 'Unknown']:
        if col not in daily_counts.columns:
            daily_counts[col] = 0
    
    # Calculate total trades per day
    daily_counts['total'] = daily_counts[['Limit', 'Market', 'Unknown']].sum(axis=1)
    
    # Calculate fractions
    daily_counts['limit_fraction'] = daily_counts['Limit'] / daily_counts['total']
    daily_counts['market_fraction'] = daily_counts['Market'] / daily_counts['total']
    daily_counts['unknown_fraction'] = daily_counts['Unknown'] / daily_counts['total']
    
    # Sort by date
    daily_counts = daily_counts.sort_index()
    
    return daily_counts


def print_summary(df: pd.DataFrame, daily_fractions: pd.DataFrame):
    """Print summary statistics."""
    
    if df.empty or daily_fractions.empty:
        print("No data to summarize")
        return
    
    print("=" * 80)
    print("ORDER TYPE FRACTION ANALYSIS")
    print("=" * 80)
    
    print(f"\nAnalysis Period:")
    print(f"  Start: {df['trade_date'].min()}")
    print(f"  End:   {df['trade_date'].max()}")
    print(f"  Total trading days: {len(daily_fractions)}")
    
    print(f"\nTotal Trades: {len(df):,}")
    print(f"  With known order type: {df['orderType'].isin(['Limit', 'Market']).sum():,}")
    print(f"  With unknown order type: {(df['orderType'] == 'Unknown').sum():,}")
    
    print(f"\nOverall Order Type Distribution:")
    overall_counts = df['orderType'].value_counts()
    for order_type, count in overall_counts.items():
        pct = count / len(df) * 100
        print(f"  {order_type:10s}: {count:6,} trades ({pct:5.1f}%)")
    
    print(f"\nDaily Statistics (Fraction of Limit Orders):")
    print(f"  Mean:   {daily_fractions['limit_fraction'].mean():.3f} ({daily_fractions['limit_fraction'].mean()*100:.1f}%)")
    print(f"  Median: {daily_fractions['limit_fraction'].median():.3f} ({daily_fractions['limit_fraction'].median()*100:.1f}%)")
    print(f"  Std:    {daily_fractions['limit_fraction'].std():.3f}")
    print(f"  Min:    {daily_fractions['limit_fraction'].min():.3f} ({daily_fractions['limit_fraction'].min()*100:.1f}%) on {daily_fractions['limit_fraction'].idxmin()}")
    print(f"  Max:    {daily_fractions['limit_fraction'].max():.3f} ({daily_fractions['limit_fraction'].max()*100:.1f}%) on {daily_fractions['limit_fraction'].idxmax()}")
    
    print(f"\nRecent 7-Day Trend (Limit Order Fraction):")
    recent = daily_fractions.tail(7)
    for date, row in recent.iterrows():
        print(f"  {date}: {row['limit_fraction']:.3f} ({row['limit_fraction']*100:5.1f}%) - {row['total']} total trades")


def main():
    print("Loading trades data with order type information...")
    df = load_trades_with_order_type(days_back=30)
    
    if df.empty:
        return
    
    print(f"Loaded {len(df):,} trades")
    
    # Calculate daily fractions
    daily_fractions = calculate_daily_fractions(df)
    
    # Print summary
    print_summary(df, daily_fractions)
    
    # Save to CSV for further analysis
    output_path = "/tmp/order_type_fractions.csv"
    daily_fractions.to_csv(output_path)
    print(f"\nDaily fractions saved to: {output_path}")
    
    # Show the full daily breakdown
    print(f"\nDaily Breakdown (first 10 days):")
    print(daily_fractions.head(10)[['Limit', 'Market', 'Unknown', 'total', 'limit_fraction']].round(4))


if __name__ == "__main__":
    main()
