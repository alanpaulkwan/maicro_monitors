# Live Tables Coverage Analysis

## Summary: YES, All Live Tables Have Major Gaps

| Table | Rows | Coverage | Date Range | Status |
|-------|------|----------|------------|--------|
| **live_positions** | 16,307 | **66.3%** | Sep 4 - Dec 7 (95 days) | ⚠️ 32 missing days |
| **live_account** | 326 | **66.3%** | Sep 4 - Dec 7 (95 days) | ⚠️ 32 missing days |
| **orders** | 1,011 | **55.4%** | Oct 4 - Dec 7 (65 days) | ⚠️ 29 missing days |
| **live_trades** | 244 | **100%** | Sep 4 only | ❌ ONLY 1 DAY! |

## Critical Findings

### 1. live_trades is Essentially Useless
- **Only 244 records from a single day** (Sep 4, 2025)
- Despite having `dry_run=0` filter mentioned in docs, no other days exist
- **Cannot use for PnL tracking** - need to rely on live_positions instead

### 2. All Tables Share the SAME Gap Pattern
```
Common gaps (all tables):
- Oct 1, 11, 13, 22, 24, 29 (single days)
- Nov 2 (single day)
- Nov 7/9 - Dec 1 (23-25 days) ⚠️ MAJOR OUTAGE
```

This suggests a **system-wide logging issue**, not individual table problems.

### 3. live_positions and live_account Move in Lockstep
- **Identical coverage: 66.3% (63/95 days)**
- **Same exact missing dates** (32 gaps)
- Suggests they're logged by the same process/cron job

### 4. orders Table Started Later
- First record: **Oct 4** (vs Sep 4 for others)
- Slightly worse coverage (55.4% vs 66.3%)
- But shares the same Nov 7-Dec 1 outage

## Major Outages

### Outage 1: Nov 7 - Dec 1 (25 days)
- Affects: live_positions, live_account, orders
- **Cannot diagnose any execution during this period**
- Most recent and longest gap

### Outage 2: Various single-day gaps in Oct-Nov
- Oct 1, 11, 13, 22, 24, 29
- Nov 2
- Likely individual days when the system didn't run

## Implications for PnL Diagnosis

### What We CAN Analyze:
✅ **Sep 4 - Oct 31** (with gaps): ~55 days
✅ **Dec 2 - Dec 7**: 6 days  
✅ **Total usable: ~60 days**

### What We CANNOT Analyze:
❌ **Nov 5 - Nov 30**: 26-day complete blackout
❌ **live_trades**: Only 1 day of data (useless)
❌ Any individual gap days (7-8 days scattered through Oct)

### Recommended Data Sources for PnL:

**For actual PnL:**
1. `live_account` (NAV/equity) - 66% coverage
2. Derive from `live_positions` weights × returns - 66% coverage

**For execution diagnosis:**
1. `live_positions.target` vs `live_positions.current` - best source
2. `orders` table - for seeing what was actually sent to exchange
3. ❌ Don't use `live_trades` - only 1 day of data

**For model targets:**
1. `positions_jianan_v6` - 373 days, comprehensive

## Data Quality Issues Found

### 1. Dec 2, 2025 Anomaly
- `live_positions`: 1,131 rows (vs typical 60-70)
- `live_account`: 21 rows (vs typical 1)
- Suggests multiple rapid updates or logging bug

### 2. Inconsistent Update Frequency
- Most days: 1-6 updates
- Dec 2: 20+ updates (anomaly)
- Recent days (Dec 3-7): 1 update each (normalized?)

### 3. live_trades Never Updated
- Only Sep 4, 2025 exists
- Either:
  - Logging stopped after first day
  - Process changed to not log trades
  - dry_run flag preventing logging

## Recommendations

### For Immediate Use:
1. **Use diagnose_execution_opus.py ONLY for:**
   - Sep 4 - Oct 31 (with 7 gap days)
   - Dec 2 - Dec 7
2. **Add date validation** to warn when no data exists
3. **Focus on live_positions** (target vs current) for execution analysis

### For Future Improvements:
1. **Fix live_trades logging** - currently useless with 1 day
2. **Investigate Nov 7 - Dec 1 outage** - 25 days of no data
3. **Stabilize logging frequency** - Dec 2 anomaly suggests instability
4. **Add monitoring alerts** when logging stops for >1 day

### For PnL Attribution:
```python
# Reliable periods for three-way comparison:
# positions_jianan_v6 → live_positions.target → live_positions.current

Good periods:
- 2025-09-04 to 2025-09-30 (27 days)
- 2025-10-02 to 2025-10-31 (22 days, some gaps)
- 2025-12-02 to 2025-12-07 (6 days)

Total analyzable: ~55 days
```
