#!/usr/bin/env python3
"""
Run SQL directly to verify results - NO HALLUCINATION
"""

from modules.clickhouse_client import query_df

# Exact SQL query
SQL = """
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

ORDER BY 
    CASE category 
        WHEN 'Overall Summary' THEN 1 
        WHEN 'Recent 30 Days' THEN 2 
        WHEN 'Recent 7 Days' THEN 3 
    END
"""

print("=" * 90)
print("RAW SQL QUERY RESULTS - NO HALLUCINATION")
print("=" * 90)
print()
print("SQL Query:")
print(SQL)
print()
print("=" * 90)
print("RESULTS:")
print("=" * 90)
print()

df = query_df(SQL)

# Format as table
print(f"{'Category':<20} {'Total':<10} {'Limit':<10} {'Market':<10} {'Limit%':<10} {'Market%':<10}")
print("-" * 90)

for _, row in df.iterrows():
    print(f"{row['category']:<20} {row['total_orders']:<10} {row['limit_orders']:<10} {row['market_orders']:<10} {row['limit_pct']:<10.2f}% {row['market_pct']:<10.2f}%")

print()
print("=" * 90)
