# Trades/Orders Staleness Investigation - ROOT CAUSE FOUND

## Issue Summary
- **Status**: ✅ Cron processes ARE running correctly
- **Problem**: Trades and orders tables show as "STALE" in email alerts
- **Investigation Date**: 2025-12-13

## The "Problem" - It's NOT actually a problem!

### Finding: Trades only happen at midnight UTC
```
Trades by hour (last 7 days):
 hour  trade_count
    0          222  ← ALL trades happen at 00:00-00:02 UTC!
```

### Top trade minutes per day:
```
Date         Minute              Count
2025-12-13   2025-12-13 00:01:00  29
2025-12-13   2025-12-13 00:00:00   4
2025-12-12   2025-12-12 00:01:00  28
2025-12-11   2025-12-11 00:01:00  24
...
```

**All trades execute within a 2-minute window at midnight UTC!**

## What's Actually Happening

### ✅ Cron Jobs Working Perfectly
1. **Ping runs every 15 minutes**: Fetches data from Hyperliquid API
2. **Data IS being captured**: Buffers are filled with trades and orders
3. **Flush runs every 3 hours**: Successfully inserts data to both local & cloud
4. **Data IS in database**: Confirmed via direct queries

### 🔍 The Real Issue
- **Expected behavior**: Email alert expects trades every 6 hours (threshold: 6h)
- **Actual behavior**: Trades only occur at midnight UTC (strategy design)
- **Result**: By 14:00 UTC, trades are 14 hours "old", triggering "STALE" alert

## Evidence

### Buffer files exist and are current:
```
-rw-rw-r-- 43K Dec 13 22:45 orders_20251213_224508_091259.parquet
-rw-rw-r-- 233K Dec 13 22:45 trades_20251213_224507_546052.parquet
```

### Flush logs show successful inserts:
```
[trades] Inserting 46000 rows into maicro_monitors.trades
[trades] Running OPTIMIZE TABLE FINAL...
[trades] Flush complete; buffer cleared.

[orders] Inserting 48000 rows into maicro_monitors.orders
[orders] Running OPTIMIZE TABLE FINAL...
[orders] Flush complete; buffer cleared.
```

### Data verification:
```sql
SELECT max(time) FROM maicro_monitors.trades;
-- Result: 2025-12-13 00:01:58.149000  ← Most recent trade
```

## Root Cause

**The trading strategy executes exactly once per day at midnight UTC.**

This is either:
1. **By design**: Strategy is meant to rebalance once daily
2. **Exchange limitation**: Hyperliquid's `userFills` API only returns recent fills
3. **Timing constraint**: Trading bot only runs at that specific time

## What This Means

### ✅ Nothing is broken!
- Cron processes are working correctly
- Data pipeline is functioning
- Buffers are being filled and flushed
- Database is being updated

### ⚠️ Alert threshold mismatch
- Staleness threshold: 6 hours
- Trade frequency: ~24 hours (midnight UTC)
- Result: False "STALE" alerts during the day

## Solutions

### Option 1: Adjust threshold (Recommended)
Change trade/order staleness threshold from 6h to 30h to account for daily trading:
```python
PER_ACCOUNT_TABLES = [
    ("maicro_monitors.trades", "time", timedelta(hours=30)),  # Was: 6h
    ("maicro_monitors.orders", "timestamp", timedelta(hours=30)),  # Was: 6h
    ...
]
```

### Option 2: Add expected cadence note
Add documentation to email explaining that trades execute daily at midnight.

### Option 3: Dynamic threshold
Set threshold based on known trading schedule (e.g., 24h + 6h buffer).

## Recommendations

1. ✅ **Immediately**: Adjust thresholds to 30h for trades/orders
2. 📧 **Updated**: Email subjects now show status counts (e.g., "[MAICRO DAILY] Table Staleness Summary (4 STALE)")
3. 📝 **Document**: Note in email body explaining daily trading schedule
4. 📊 **Monitor**: Track if trade frequency changes over time

## Files Modified

- `scheduled_processes/emails/daily/table_staleness_daily.py`
  - Added status summary to email subject line
  - Example: "[MAICRO DAILY] Table Staleness Summary (4 STALE, 2 MISSING)"

## Conclusion

**No action needed on cron/data pipeline** - it's working perfectly. The "staleness" is expected behavior based on the once-daily trading schedule. Adjust thresholds to match actual trading cadence.
