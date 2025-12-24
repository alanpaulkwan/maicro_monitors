#!/usr/bin/env python3
"""
Order Type Fraction Over Time - Comprehensive Analysis

This script calculates and visualizes the fraction of limit vs market orders over time,
showing trends in execution strategy.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import numpy as np

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from modules.clickhouse_client import query_df


def load_order_type_data(days_back: Optional[int] = None) -> pd.DataFrame:
    """Load order type data from ClickHouse."""
    
    if days_back:
        since = datetime.utcnow() - timedelta(days=days_back)
        where_clause = f"WHERE t.time >= '{since.strftime('%Y-%m-%d')}'"
    else:
        where_clause = ""
    
    sql = f"""
    SELECT 
        toDate(t.time) as trade_date,
        count(*) as total_trades,
        countIf(o.orderType = 'Limit') as limit_trades,
        countIf(o.orderType = 'Market') as market_trades,
        countIf(o.orderType IS NULL) as unknown_trades
    FROM maicro_monitors.trades t
    LEFT JOIN maicro_monitors.orders o ON t.oid = o.oid
    {where_clause}
    GROUP BY trade_date
    ORDER BY trade_date DESC
    """
    
    df = query_df(sql)
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    
    return df


def calculate_statistics(df: pd.DataFrame) -> dict:
    """Calculate summary statistics."""
    
    # Overall stats
    total_trades = df['total_trades'].sum()
    total_limit = df['limit_trades'].sum()
    total_market = df['market_trades'].sum()
    total_unknown = df['unknown_trades'].sum()
    
    # Monthly stats
    df['year_month'] = pd.to_datetime(df['trade_date']).dt.to_period('M')
    monthly = df.groupby('year_month').agg({
        'total_trades': 'sum',
        'limit_trades': 'sum',
        'market_trades': 'sum',
        'unknown_trades': 'sum'
    })
    monthly['limit_pct'] = monthly['limit_trades'] / monthly['total_trades'] * 100
    
    # Daily stats
    df['limit_pct'] = df['limit_trades'] / df['total_trades'] * 100
    df['market_pct'] = df['market_trades'] / df['total_trades'] * 100
    
    return {
        'overall': {
            'total_trades': total_trades,
            'limit_trades': total_limit,
            'market_trades': total_market,
            'unknown_trades': total_unknown,
            'limit_pct': total_limit / total_trades * 100,
            'market_pct': total_market / total_trades * 100,
            'unknown_pct': total_unknown / total_trades * 100
        },
        'monthly': monthly,
        'daily': df
    }


def print_report(stats: dict):
    """Print comprehensive report."""
    
    print("=" * 100)
    print("ORDER TYPE FRACTION ANALYSIS - EXECUTION STRATEGY TRENDS")
    print("=" * 100)
    
    # Overall statistics
    overall = stats['overall']
    print(f"\n📊 OVERALL EXECUTION STATISTICS")
    print(f"   Total trades analyzed: {overall['total_trades']:,}")
    print(f"   ├─ Limit orders:       {overall['limit_trades']:,} ({overall['limit_pct']:.1f}%)")
    print(f"   ├─ Market orders:      {overall['market_trades']:,} ({overall['market_pct']:.1f}%)")
    print(f"   └─ Unknown:            {overall['unknown_trades']:,} ({overall['unknown_pct']:.1f}%)")
    
    # Monthly breakdown
    monthly = stats['monthly']
    print(f"\n📅 MONTHLY EXECUTION BREAKDOWN")
    print(f"{'Month':<12} {'Total':>10} {'Limit%':>8} {'Market%':>9} {'Unknown%':>10}")
    print("-" * 60)
    
    for month, row in monthly.iterrows():
        limit_pct = row['limit_trades'] / row['total_trades'] * 100
        market_pct = row['market_trades'] / row['total_trades'] * 100
        unknown_pct = row['unknown_trades'] / row['total_trades'] * 100
        
        print(f"{month!s:<12} {row['total_trades']:8.0f} "
              f"{limit_pct:6.1f}% {market_pct:7.1f}% {unknown_pct:8.1f}%")
    
    # Trend analysis
    daily = stats['daily'].sort_values('trade_date')
    
    # Remove days with no data
    days_with_orders = daily[daily['total_trades'] > 0].copy()
    
    if not days_with_orders.empty:
        print(f"\n📈 TREND ANALYSIS")
        
        # Recent vs older periods
        recent_30 = days_with_orders.tail(30)
        older_30 = days_with_orders.head(30)
        
        if not recent_30.empty and not older_30.empty:
            recent_avg = recent_30['limit_pct'].mean()
            older_avg = older_30['limit_pct'].mean()
            trend = recent_avg - older_avg
            
            print(f"   Recent 30 days avg:   {recent_avg:.1f}% limit orders")
            print(f"   Oldest 30 days avg:   {older_avg:.1f}% limit orders")
            print(f"   Trend:                {trend:+.1f} percentage points")
            
            if trend > 10:
                print(f"   → Strong increase in limit order usage")
            elif trend > 5:
                print(f"   → Moderate increase in limit order usage")
            elif trend < -10:
                print(f"   → Strong decrease in limit order usage")
            elif trend < -5:
                print(f"   → Moderate decrease in limit order usage")
            else:
                print(f"   → Stable limit order usage")
        
        # Identify days with market orders
        market_days = days_with_orders[days_with_orders['market_trades'] > 0]
        if not market_days.empty:
            print(f"\n⚠️  DAYS WITH MARKET ORDERS ({len(market_days)} days)")
            print(f"{'Date':<12} {'Trades':>8} {'Limit%':>8} {'Market%':>9}")
            print("-" * 40)
            
            for _, row in market_days.iterrows():
                print(f"{row['trade_date']} {row['total_trades']:6.0f} "
                      f"{row['limit_pct']:6.1f}% {row['market_pct']:7.1f}%")
        
        # Best and worst days
        best_days = days_with_orders.nlargest(5, 'limit_pct')
        worst_days = days_with_orders.nsmallest(5, 'limit_pct')
        
        print(f"\n🏆 BEST LIMIT ORDER DAYS")
        print(f"{'Date':<12} {'Trades':>8} {'Limit%':>8}")
        print("-" * 35)
        for _, row in best_days.iterrows():
            print(f"{row['trade_date']} {row['total_trades']:6.0f} {row['limit_pct']:6.1f}%")
        
        print(f"\n📉 LOWEST LIMIT ORDER DAYS")
        print(f"{'Date':<12} {'Trades':>8} {'Limit%':>8}")
        print("-" * 35)
        for _, row in worst_days.iterrows():
            print(f"{row['trade_date']} {row['total_trades']:6.0f} {row['limit_pct']:6.1f}%")
    
    # Weekly aggregation
    daily['week'] = pd.to_datetime(daily['trade_date']).dt.to_period('W')
    weekly = daily.groupby('week').agg({
        'total_trades': 'sum',
        'limit_trades': 'sum',
        'market_trades': 'sum'
    })
    weekly['limit_pct'] = weekly['limit_trades'] / weekly['total_trades'] * 100
    
    print(f"\n📊 WEEKLY EXECUTION PATTERN")
    print(f"Recent 4 weeks:")
    print(f"{'Week':<12} {'Total':>8} {'Limit%':>8}")
    print("-" * 35)
    for week, row in weekly.head(4).iterrows():
        print(f"{week!s:<12} {row['total_trades']:6.0f} {row['limit_pct']:6.1f}%")


def save_data(df: pd.DataFrame, stats: dict):
    """Save data to CSV files."""
    
    # Save daily fractions
    daily_file = "/tmp/order_type_fractions_daily.csv"
    df.to_csv(daily_file, index=False)
    
    # Save monthly summary
    monthly_file = "/tmp/order_type_fractions_monthly.csv"
    stats['monthly'].to_csv(monthly_file)
    
    print(f"\n💾 Data saved:")
    print(f"   Daily fractions:   {daily_file}")
    print(f"   Monthly breakdown: {monthly_file}")


def generate_insights(stats: dict):
    """Generate key insights."""
    
    overall = stats['overall']
    monthly = stats['monthly']
    
    print(f"\n" + "="*100)
    print("KEY INSIGHTS")
    print("="*100)
    
    print(f"""
1. EXECUTION STRATEGY
   ├─ Primary method: {'Limit orders' if overall['limit_pct'] > 70 else 'Mixed'}
   ├─ Market order usage: {'Rare (<5%)' if overall['market_pct'] < 5 else 'Frequent (≥5%)'}
   └─ Data completeness: {'Complete' if overall['unknown_pct'] < 1 else 'Incomplete'}

2. TEMPORAL PATTERNS
   ├─ Best month for limit orders: {monthly['limit_pct'].idxmax()} ({monthly['limit_pct'].max():.1f}%)
   ├─ Most market order usage: {monthly['market_trades'].idxmax()} ({monthly['market_trades'].max()} orders)
   └─ Data gap period: {monthly[monthly['total_trades'] == 0].index.tolist() if any(monthly['total_trades'] == 0) else 'None'}

3. EXECUTION QUALITY METRICS
   ├─ Average trades per day: {monthly['total_trades'].sum() / len(stats['daily']):.1f}
   ├─ Limit order consistency: {'High (Std dev < 20%)' if stats['daily']['limit_pct'].std() < 20 else 'Variable'}
   └─ Execution discipline: {'High' if overall['market_pct'] < 5 else 'Moderate'}
    """)


def main():
    print("Loading order type data from database...")
    
    # Load all available data
    df = load_order_type_data()
    
    # Calculate statistics
    stats = calculate_statistics(df)
    
    # Print report
    print_report(stats)
    
    # Generate insights
    generate_insights(stats)
    
    # Save data
    save_data(df, stats)


if __name__ == "__main__":
    main()
