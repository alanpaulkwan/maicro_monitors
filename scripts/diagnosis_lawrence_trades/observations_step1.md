# Lawrence Trades Diagnosis - Step 1 Observations

**Date:** 2025-12-08

## Executive Summary

Analysis of trade execution quality comparing pre-fix (before Dec 2, 2025) vs post-fix periods. The primary issue was **wrong offset**: Lawrence traded T+1 instead of T+2 (i.e., used yesterday's signal instead of 2-days-ago signal).

## Waterfall Analysis

### PRE-FIX (Before Dec 2, 2025)
```
  1969 Total target positions
    ▼ - 335 not executed (17.0%)
  1634 Matched (83.0%)
    ▼ - 586 wrong sign (35.9%)
  1048 Sign OK (64.1%)
    ▼ - 348 over 2% weight diff (33.2%)
   700 SUCCESS (35.6%)

  Correct T+2 offset: 122 (7.5%)
  Wrong offset: 1512 (92.5%)
```

### POST-FIX (Dec 2, 2025 onwards)
```
   217 Total target positions
    ▼ -  30 not executed (13.8%)
   187 Matched (86.2%)
    ▼ -  65 wrong sign (34.8%)
   122 Sign OK (65.2%)
    ▼ -  33 over 2% weight diff (27.0%)
    89 SUCCESS (41.0%)

  Correct T+2 offset: 156 (83.4%)
  Wrong offset: 31 (16.6%)
```

## Key Metrics Comparison

| Metric | PRE | POST | Change |
|--------|-----|------|--------|
| Coverage (matched/total) | 83.0% | 86.2% | +3.2% |
| Sign correct (of matched) | 64.1% | 65.2% | +1.1% |
| Success rate (final) | 35.6% | 41.0% | +5.5% |
| **Correct T+2 offset** | **7.5%** | **83.4%** | **+76.0%** |

## Root Causes Identified

### 1. Wrong Offset (PRIMARY ISSUE)
- **What happened:** Lawrence used `target_date = ts - 1 day` when it should be `target_date = ts - 2 days`
- **Impact:** 92.5% of pre-fix trades used wrong signal
- **Status:** Fixed on Dec 2, 2025 (now 83.4% correct)

### 2. Wrong Sign Analysis (~35% of matched positions)

**Deep Dive Results (all dates):**
| Category | Count | % |
|----------|-------|---|
| T-2 correct (as designed) | 1233 | 71.0% |
| T-1 correct, T-2 wrong | 253 | 14.6% |
| Both T-1 and T-2 wrong | 251 | 14.5% |

**"Both Wrong" Breakdown by USD Value:**
| Bucket | Count | % | Avg USD |
|--------|-------|---|---------|
| Residual (<$1) | 165 | 84.2% | $0 |
| Small ($10-50) | 16 | 8.2% | $24 |
| Significant (>$50) | 15 | 7.7% | $113 |

**Root Cause of Significant "Both Wrong":**
- Many are from early dates (Sept-Oct 2025) when lag was worse
- Often match T-3 or T-4 signals (system was 3-4 days behind, not just 1)
- Some symbols have `nan` weights (not in model at all but held anyway)

### 3. Not Executed (~15% of targets)
- Target positions that don't appear in live_positions
- **NOT due to min notional:** All symbols have min_usd=$10, which is 0.5% of $2000 portfolio
- The model outputs ~500 symbols, but we only trade top 32 by |weight|
- Comparing top 32: some mismatches (4 missing, 4 extra on 2025-12-05)
- **Cause:** Symbol case mismatch (model=lowercase, live_positions=UPPERCASE) was fixed in analysis
- **Remaining cause:** Actual execution selects different symbols than top 32

### 4. Weight Magnitude Errors (~30% of sign-correct)
- Positions with correct sign but >2% weight difference
- Improved slightly post-fix (33.2% → 27.0%)

## Methodology

### Alignment Logic
- `ts` = holdings date from `live_positions` (kind='current')
- `target_date` = `ts - 2 days` (correct offset for T+2 settlement)
- Target weights from `positions_jianan_v6` where `inserted_at < ts::date`
- Actual weights = position value / total portfolio value (normalized to sum to 1 per side)

### Definitions
- **Matched:** Symbol appears in both target and actual for that date
- **Wrong sign:** `sign(target_weight) != sign(actual_weight)`
- **Over 2%:** `|target_weight - actual_weight| > 0.02` (2% of portfolio)
- **Correct offset:** The signal used was actually the T-2 signal

## Next Steps

1. ~~Investigate persistent wrong-sign issues (35% rate unchanged)~~ **DONE** - see detailed breakdown above
2. ~~Analyze minimum notional filter impact on not-executed positions~~ **DONE** - not the cause
3. Deep dive into weight magnitude errors
4. Verify Dec 2+ period has stabilized
5. **NEW:** Investigate why some symbols are traded that aren't in top 32
6. **NEW:** Investigate early-period (Sept-Oct) positions matching T-3/T-4 signals

## Key Findings Summary

1. **Symbol Case Issue:** Model outputs lowercase symbols, live_positions has UPPERCASE - must use `upper()` for joins
2. **Residual Positions:** 84% of "both wrong" are $0 residuals - not actually wrong, just artifacts
3. **Early Period Lag:** Sept-Oct 2025 had worse lag (T-3/T-4), not just T-1
4. **Min Notional:** NOT a factor - min_usd=$10 = 0.5% threshold is rarely hit

## Scripts

- `waterfall_sql.py` - Parameterized waterfall analysis (pure SQL)
- `build_aligned_timeseries.py` - Builds symbol/date aligned comparison
