#!/usr/bin/env python3
"""
Visualize Order Type Fraction Over Time

Creates a simple text-based visualization of order type fractions over time.
"""

import pandas as pd
from datetime import datetime, timedelta
from modules.clickhouse_client import query_df


def create_text_chart():
    """Create a text-based chart showing limit order percentage over time."""
    
    # Load data
    sql = """
    SELECT 
        toDate(t.time) as trade_date,
        count(*) as total_trades,
        countIf(o.orderType = 'Limit') as limit_trades,
        countIf(o.orderType = 'Market') as market_trades,
        countIf(o.orderType IS NULL) as unknown_trades,
        round(100.0 * countIf(o.orderType = 'Limit') / count(*), 2) as limit_pct
    FROM maicro_monitors.trades t
    LEFT JOIN maicro_monitors.orders o ON t.oid = o.oid
    GROUP BY trade_date
    ORDER BY trade_date DESC
    """
    
    df = query_df(sql)
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    df = df.sort_values('trade_date')
    
    print("=" * 80)
    print("ORDER TYPE FRACTION TIMELINE")
    print("=" * 80)
    print("Each bar represents a day, showing the percentage of trades executed via limit orders")
    print("=" * 80)
    
    # Group by month for display
    df['year_month'] = pd.to_datetime(df['trade_date']).dt.to_period('M')
    
    for month, month_data in df.groupby('year_month'):
        print(f"\n{month}:")
        print("Date       Limit% | Chart")
        print("-" * 50)
        
        for _, row in month_data.iterrows():
            # Create bar chart
            bar_length = int(row['limit_pct'] / 2)  # Scale to 0-50 chars
            bar = "█" * bar_length + "░" * (50 - bar_length)
            
            # Color code based on percentage
            if row['limit_pct'] >= 90:
                indicator = "🔵"  # High limit usage
            elif row['limit_pct'] >= 50:
                indicator = "🟢"  # Moderate limit usage
            elif row['limit_pct'] > 0:
                indicator = "🟡"  # Low limit usage
            else:
                indicator = "🔴"  # No limit orders/no data
            
            print(f"{row['trade_date']} {row['limit_pct']:6.1f}% {indicator} |{bar}| ({row['total_trades']:.0f} trades)")
    
    # Summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    # Filter out days with no trades
    valid_days = df[df['total_trades'] > 0].copy()
    
    if not valid_days.empty:
        print(f"Average limit order usage: {valid_days['limit_pct'].mean():.1f}%")
        print(f"Best day: {valid_days['limit_pct'].max():.1f}% on {valid_days.loc[valid_days['limit_pct'].idxmax(), 'trade_date']}")
        print(f"Worst day: {valid_days['limit_pct'].min():.1f}% on {valid_days.loc[valid_days['limit_pct'].idxmin(), 'trade_date']}")
        
        # Show recent trend
        recent = valid_days.tail(7)
        print(f"\nLast 7 days average: {recent['limit_pct'].mean():.1f}%")
        
        # Count days with different patterns
        all_limit = len(valid_days[valid_days['limit_pct'] >= 99.9])
        mixed = len(valid_days[(valid_days['limit_pct'] > 0) & (valid_days['limit_pct'] < 99.9)])
        no_limit = len(valid_days[valid_days['limit_pct'] == 0])
        
        print(f"\nDays with 100% limit orders: {all_limit} days")
        print(f"Days with mixed execution:    {mixed} days")
        print(f"Days with 0% limit orders:    {no_limit} days")
        print(f"Total trading days analyzed:  {len(valid_days)} days")
        
        # Market order usage
        market_days = valid_days[valid_days['market_trades'] > 0]
        if not market_days.empty:
            print(f"\n⚠️  Market order usage detected:")
            print(f"   Total market orders: {market_days['market_trades'].sum()}")
            print(f"   Days with market orders: {len(market_days)} days")
            print(f"   Highest single day: {market_days['market_trades'].max()} market orders")
    
    print("=" * 80)


def create_detailed_timeline():
    """Create a more detailed timeline view."""
    
    print("\n" + "=" * 100)
    print("DETAILED EXECUTION TIMELINE")
    print("=" * 100)
    
    sql = """
    SELECT 
        toDate(t.time) as trade_date,
        count(*) as total_trades,
        countIf(o.orderType = 'Limit') as limit_trades,
        countIf(o.orderType = 'Market') as market_trades,
        countIf(o.orderType IS NULL) as unknown_trades,
        sum(t.sz * t.px) as total_notional
    FROM maicro_monitors.trades t
    LEFT JOIN maicro_monitors.orders o ON t.oid = o.oid
    GROUP BY trade_date
    ORDER BY trade_date
    """
    
    df = query_df(sql)
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    
    # Calculate percentages
    df['limit_pct'] = df['limit_trades'] / df['total_trades'] * 100
    df['market_pct'] = df['market_trades'] / df['total_trades'] * 100
    df['unknown_pct'] = df['unknown_trades'] / df['total_trades'] * 100
    
    print(f"{'Date':<12} {'Trades':>8} {'Limit':>8} {'Market':>8} {'Limit%':>8} {'Notional':>12}")
    print("-" * 80)
    
    for _, row in df.iterrows():
        if row['total_trades'] > 0:
            print(f"{row['trade_date']} "
                  f"{row['total_trades']:6.0f} "
                  f"{row['limit_trades']:6.0f} "
                  f"{row['market_trades']:6.0f} "
                  f"{row['limit_pct']:6.1f}% "
                  f"${row['total_notional']:8.0f}")


if __name__ == "__main__":
    create_text_chart()
    create_detailed_timeline()
