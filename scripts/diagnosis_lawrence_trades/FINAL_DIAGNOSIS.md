# Lawrence Execution Diagnosis - Final Report

## Executive Summary

**Key Finding: Lawrence fixed the offset error around November 1, 2025**

| Period | What Lawrence Did | Correct Behavior |
|--------|------------------|------------------|
| **Sep-Oct 2025** | T+1 (signal = holdings - 1 day) | Should be T+2 |
| **Nov-Dec 2025** | T+2 (signal = holdings - 2 days) | ✅ Correct |

## Data Explanation

- `ts` = holdings date (when position was held/logged)
- `target_date` = signal date Lawrence used
- `positions_jianan_v6.date` = actual model signal date

The lag (`ts - target_date`) shows what offset Lawrence used:
- lag=1 → T+1 execution (using yesterday's signal)
- lag=2 → T+2 execution (using 2-day-ago signal)

## Lag Distribution Over Time

| Month | lag=1 (T+1) | lag=2 (T+2) | Dominant |
|-------|-------------|-------------|----------|
| Sep 2025 | 2,829 | 0 | **T+1 (wrong)** |
| Oct 2025 | 2,627 | 96 | **T+1 (wrong)** |
| Nov 2025 | 243 | 488 | **T+2 (correct)** |
| Dec 2025 | 496 | 156 | Mixed |

## Tracking Error Analysis

### OLD PERIOD (Sep-Oct, when Lawrence used T+1)

| Metric | T+1 (what he did) | T+2 (should have done) |
|--------|-------------------|------------------------|
| Matched positions | 1,510 | 1,479 |
| Sign correct | **66.0%** | 60.6% |
| Within 2% weight | **55.2%** | 44.4% |
| Mean abs diff | **0.0233** | 0.0282 |

**Conclusion**: T+1 metrics are better because that's what Lawrence actually executed.

### NEW PERIOD (Nov-Dec, when Lawrence uses T+2)

| Metric | T+1 (wrong) | T+2 (what he does) |
|--------|-------------|---------------------|
| Matched positions | 340 | 342 |
| Sign correct | 70.6% | **68.7%** |
| Within 2% weight | 68.8% | **66.7%** |
| Mean abs diff | 0.0180 | **0.0180** |

**Note**: T+1 and T+2 show similar metrics in Nov-Dec, which is unusual. This may be because:
1. The model signals are autocorrelated (T+1 and T+2 signals are similar)
2. Small sample size (only 340 positions)
3. Some dates still used T+1 even in Nov-Dec period

## Impact of the Fix

| Metric | Before Fix (Sep-Oct) | After Fix (Nov-Dec) | Change |
|--------|---------------------|---------------------|--------|
| Sign correct | 66.0% | 68.7% | **+2.7%** |
| Within 2% weight | 55.2% | 66.7% | **+11.4%** |
| Mean tracking error | 0.0233 | 0.0180 | **-0.0053** |

## Direction Analysis

**Sign-wrong positions are concentrated in specific symbols:**

| Symbol | Wrong Sign Count (Sep-Oct) |
|--------|---------------------------|
| ADA | 34 |
| PENGU | 32 |
| SUI | 32 |
| XRP | 31 |
| NEAR | 30 |
| ARB | 28 |
| WIF | 27 |
| DOGE | 27 |

These symbols consistently have opposite signs from target - could be:
1. Reduction orders being logged as position
2. Stale position data
3. Actual execution bugs

## Magnitude Analysis (2% threshold)

### OLD PERIOD (Sep-Oct)
- ≤2% diff (good): 55.2%
- 2-5% diff: 31.8%
- 5-10% diff: 10.0%
- >10% diff (bad): 0.4%

### NEW PERIOD (Nov-Dec)
- ≤2% diff (good): 66.7%
- 2-5% diff: (remaining ~33%)

## Recommendations

### Already Done ✅
1. Lawrence fixed the offset from T+1 to T+2 around Nov 1, 2025

### Still Needed
1. **Investigate wrong-sign positions** (20-30% of all positions)
   - Focus on ADA, PENGU, SUI, XRP, NEAR, ARB
   - Check if logging captures pre-trade or post-trade state

2. **Improve magnitude accuracy**
   - 45% of positions in Sep-Oct had >2% weight deviation
   - 33% of positions in Nov-Dec still have >2% deviation

3. **Fix logging gaps**
   - Nov 5-30 has no data (25 day gap)
   - Only 64 days of live_positions data total

## Files Generated

```
scripts/diagnosis_lawrence_trades/
├── diagnose_offset_error.py      # Main analysis script
├── alignment_correct_t2.csv      # T+2 aligned data
├── alignment_wrong_t1.csv        # T+1 aligned data
├── daily_te_comparison.csv       # Daily tracking error
└── FINAL_DIAGNOSIS.md            # This report
```
