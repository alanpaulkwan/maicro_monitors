#!/usr/bin/env python3
"""
ONE-OFF SCRIPT: Check Order Types (Limit vs Market)

This script queries the local chenlin04.fbe.hku.hk database to determine
whether we're using limit orders, market orders, or both.

Location: scripts/adhoc/ (one-off analysis scripts)
Uses credentials from: config/local_secrets.json
"""

import os
import sys
from pathlib import Path

# Add repo root to path for imports
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(REPO_ROOT))

from modules.clickhouse_client import query_df

# SQL query to check order types
ORDER_TYPE_QUERY = """
SELECT 
    'Overall Summary' as category,
    COUNT(*) as total_orders,
    SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) as limit_orders,
    SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) as market_orders,
    SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) as unknown_orders,
    ROUND(SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as limit_pct,
    ROUND(SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as market_pct,
    ROUND(SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as unknown_pct
FROM maicro_monitors.orders

UNION ALL

SELECT 
    'Recent 7 Days' as category,
    COUNT(*) as total_orders,
    SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) as limit_orders,
    SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) as market_orders,
    SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) as unknown_orders,
    ROUND(SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as limit_pct,
    ROUND(SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as market_pct,
    ROUND(SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as unknown_pct
FROM maicro_monitors.orders
WHERE timestamp >= now() - INTERVAL 7 DAY

UNION ALL

SELECT 
    'Recent 30 Days' as category,
    COUNT(*) as total_orders,
    SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) as limit_orders,
    SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) as market_orders,
    SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) as unknown_orders,
    ROUND(SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as limit_pct,
    ROUND(SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as market_pct,
    ROUND(SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as unknown_pct
FROM maicro_monitors.orders
WHERE timestamp >= now() - INTERVAL 30 DAY

ORDER BY 
    CASE category 
        WHEN 'Overall Summary' THEN 1 
        WHEN 'Recent 30 Days' THEN 2 
        WHEN 'Recent 7 Days' THEN 3 
    END
"""

# Daily breakdown query
DAILY_BREAKDOWN_QUERY = """
SELECT 
    toDate(timestamp) as order_date,
    COUNT(*) as daily_orders,
    SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) as limit_orders,
    SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) as market_orders,
    SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) as unknown_orders,
    ROUND(SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as limit_pct,
    ROUND(SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as market_pct
FROM maicro_monitors.orders
WHERE timestamp >= now() - INTERVAL 30 DAY
GROUP BY order_date
ORDER BY order_date DESC
"""

def main():
    print("=" * 80)
    print("ORDER TYPE ANALYSIS - Limit vs Market Orders")
    print("Database: chenlin04.fbe.hku.hk")
    print("Table: maicro_monitors.orders")
    print("=" * 80)
    print()
    
    try:
        # Run summary query
        print("📊 SUMMARY (All Time vs Recent)")
        print("-" * 80)
        summary_df = query_df(ORDER_TYPE_QUERY)
        
        if summary_df.empty:
            print("No data found in maicro_monitors.orders")
            return
        
        # Format and display summary
        for _, row in summary_df.iterrows():
            print(f"\n{row['category']}:")
            print(f"  Total orders:     {row['total_orders']:,}")
            print(f"  ├─ Limit orders:  {row['limit_orders']:,} ({row['limit_pct']:.2f}%)")
            print(f"  ├─ Market orders: {row['market_orders']:,} ({row['market_pct']:.2f}%)")
            print(f"  └─ Unknown:       {row['unknown_orders']:,} ({row['unknown_pct']:.2f}%)")
        
        print()
        print("=" * 80)
        print()
        
        # Run daily breakdown
        print("📅 DAILY BREAKDOWN (Last 30 Days)")
        print("-" * 80)
        daily_df = query_df(DAILY_BREAKDOWN_QUERY)
        
        if not daily_df.empty:
            print(f"{'Date':<12} {'Total':<8} {'Limit':<8} {'Market':<8} {'Limit%':<8} {'Market%':<8}")
            print("-" * 80)
            
            for _, row in daily_df.head(10).iterrows():
                print(f"{row['order_date']} "
                      f"{row['daily_orders']:<7} "
                      f"{row['limit_orders']:<7} "
                      f"{row['market_orders']:<7} "
                      f"{row['limit_pct']:>6.2f}% "
                      f"{row['market_pct']:>6.2f}%")
            
            if len(daily_df) > 10:
                print(f"\n... and {len(daily_df) - 10} more days")
        
        print()
        print("=" * 80)
        print("✅ Analysis complete!")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Error running query: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure ClickHouse is running on chenlin04.fbe.hku.hk")
        print("2. Check that config/local_secrets.json has correct password")
        print("3. Verify maicro_monitors.orders table exists")
        sys.exit(1)

if __name__ == "__main__":
    main()
