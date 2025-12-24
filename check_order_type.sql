-- Query to check if we're using Limit or Market orders
-- Runs on local database: chenlin04.fbe.hku.hk

SELECT 
    'Overall Summary' as category,
    COUNT(*) as total_orders,
    SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) as limit_orders,
    SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) as market_orders,
    SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) as unknown_orders,
    ROUND(SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as limit_pct,
    ROUND(SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as market_pct,
    ROUND(SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as unknown_pct
FROM maicro_monitors.orders

UNION ALL

SELECT 
    'Recent 7 Days' as category,
    COUNT(*) as total_orders,
    SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) as limit_orders,
    SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) as market_orders,
    SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) as unknown_orders,
    ROUND(SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as limit_pct,
    ROUND(SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as market_pct,
    ROUND(SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as unknown_pct
FROM maicro_monitors.orders
WHERE timestamp >= now() - INTERVAL 7 DAY

UNION ALL

SELECT 
    'Recent 30 Days' as category,
    COUNT(*) as total_orders,
    SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) as limit_orders,
    SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) as market_orders,
    SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) as unknown_orders,
    ROUND(SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as limit_pct,
    ROUND(SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as market_pct,
    ROUND(SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as unknown_pct
FROM maicro_monitors.orders
WHERE timestamp >= now() - INTERVAL 30 DAY

ORDER BY category;

-- Daily breakdown for the last 30 days
SELECT 
    toDate(timestamp) as order_date,
    COUNT(*) as daily_orders,
    SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) as limit_orders,
    SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) as market_orders,
    SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) as unknown_orders,
    ROUND(SUM(CASE WHEN orderType = 'Limit' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as limit_pct,
    ROUND(SUM(CASE WHEN orderType = 'Market' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as market_pct,
    ROUND(SUM(CASE WHEN orderType IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as unknown_pct
FROM maicro_monitors.orders
WHERE timestamp >= now() - INTERVAL 30 DAY
GROUP BY order_date
ORDER BY order_date DESC;
