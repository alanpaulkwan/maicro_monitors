# Waterfall Breakdown: Lawrence Execution Diagnosis

## Usage

```bash
# Full analysis (all dates)
python waterfall_diagnosis.py --compare

# Post-fix period only (Dec 2025+)
python waterfall_diagnosis.py --start_date 2025-12-01 --compare

# Custom threshold
python waterfall_diagnosis.py --threshold 0.01  # 1%

# Future analysis (after Dec 2)
python waterfall_diagnosis.py --start_date 2025-12-02 --end_date 2025-12-31
```

## Waterfall Results

### ALL DATA (Sep 4 - Dec 7, 2025)

```
    2,186 Total targets
       │
       ▼ -365 not executed (16.7%)
    1,821 Matched (83.3%)
       │
       ▼ -690 wrong sign (37.9%)
    1,131 Sign correct (62.1%)
       │
       ▼ -431 over 2%
      700 SUCCESS (32.0% end-to-end)

OFFSET ATTRIBUTION:
  ✓ Used correct offset (T+2): 278 (15.3%)
  ✗ Used wrong offset (T+1):   1,543 (84.7%)  ← Most data is pre-fix
```

### POST-FIX ONLY (Dec 1 - Dec 7, 2025)

```
      217 Total targets
       │
       ▼ -30 not executed (13.8%)
      187 Matched (86.2%)
       │
       ▼ -55 wrong sign (29.4%)
      132 Sign correct (70.6%)
       │
       ▼ -19 over 2%
      113 SUCCESS (52.1% end-to-end)

OFFSET ATTRIBUTION:
  ✓ Used correct offset (T+2): 156 (83.4%)  ← Lawrence fixed it!
  ✗ Used wrong offset (T+1):    31 (16.6%)
```

## Offset Comparison

### ALL DATA

| Metric | T+2 (correct) | T+1 (wrong) | Diff |
|--------|--------------|-------------|------|
| Sign correct % | 62.1% | 66.9% | +4.8% |
| Within 2% | 61.9% | 70.5% | +8.6% |
| End-to-end | 32.0% | 39.8% | +7.8% |
| Mean TE | 0.0191 | 0.0156 | -0.0035 |

**Note**: T+1 looks "better" because most of the data is from Sep-Oct when Lawrence was actually using T+1.

### POST-FIX ONLY (Dec)

| Metric | T+2 (correct) | T+1 (wrong) | Diff |
|--------|--------------|-------------|------|
| Sign correct % | **70.6%** | 65.6% | **-5.0%** |
| Within 2% | **85.6%** | 75.0% | **-10.6%** |
| End-to-end | **52.1%** | 41.1% | **-11.0%** |
| Mean TE | **0.0094** | 0.0148 | **+0.0054** |

**Now T+2 is clearly better** because Lawrence is actually using T+2 now.

## Loss Attribution

### ALL DATA
| Stage | Count | % of Losses |
|-------|-------|-------------|
| Not executed | 365 | 24.6% |
| Wrong sign | 690 | **46.4%** |
| Wrong size | 431 | 29.0% |

### POST-FIX
| Stage | Count | % of Losses |
|-------|-------|-------------|
| Not executed | 30 | 28.8% |
| Wrong sign | 55 | **52.9%** |
| Wrong size | 19 | 18.3% |

**Key Finding**: Wrong sign is still the biggest problem (46-53% of losses), even after fixing the offset.

## Improvement from Fix

| Metric | Before Fix (Sep-Oct) | After Fix (Dec) | Change |
|--------|---------------------|-----------------|--------|
| Coverage | 83.3% | 86.2% | +2.9% |
| Sign correct | 62.1% | 70.6% | **+8.5%** |
| Within 2% | 61.9% | 85.6% | **+23.7%** |
| End-to-end | 32.0% | 52.1% | **+20.1%** |
| Mean TE | 0.0191 | 0.0094 | **-0.0097** |

## Top Problem Symbols (Wrong Sign)

### All Data
ADA (34), PENGU (32), SUI (32), XRP (31), NEAR (30), ARB (28)

### Post-Fix (Dec)
APT (4), ZEN (4), FIL (3), LINK (3), WLD (3), VIRTUAL (3)

## Files

- `waterfall_diagnosis.py` - Parameterized script for analysis
- `waterfall_detailed.csv` - Detailed position-level data from last run
- `WATERFALL_SUMMARY.md` - This file
