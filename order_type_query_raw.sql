-- SQL Query to check Limit vs Market orders on chenlin04.fbe.hku.hk
-- Querying: maicro_monitors.orders table

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
    END;
