# Live Positions Analysis - Key Findings

## Question: What 'kind' values exist in live_positions?

**Answer: Only 2 values:**
- `current` - Actual positions held in the account after execution
- `target` - Desired positions from the strategy/executor

## Critical Discovery: Three-Way Comparison Needed

Looking at 2025-12-05 sample data:

| Symbol | Target Qty | Current Qty | Direction Match? | Magnitude Match? |
|--------|-----------|-------------|------------------|------------------|
| ADA    | +40.25    | **-38.00**  | ❌ OPPOSITE      | ❌ Wrong sign    |
| BTC    | +0.00125  | +0.00133    | ✓ Long           | ✓ Close          |
| DOGE   | +118.10   | +333.00     | ✓ Long           | ❌ 2.8x too much |
| ETH    | +0.02456  | +0.01980    | ✓ Long           | ⚠️ 20% under     |
| SOL    | +0.56135  | +0.67000    | ✓ Long           | ⚠️ 19% over      |

## The Real Failure Taxonomy

We need a **three-way comparison**:

```
positions_jianan_v6 (model signal)
         ↓
live_positions.target (what executor tried to achieve)
         ↓
live_positions.current (what actually happened)
```

### Revised Failure Modes:

**Stage 1: Model → Executor Target**
- `model_not_sent`: jianan_v6 has weight but no live_positions.target
- `model_altered`: target exists but different sign/magnitude than model

**Stage 2: Executor Target → Actual Position**
- `target_not_achieved`: target exists but no current
- `execution_wrong_sign`: current has opposite sign from target (see ADA above!)
- `execution_wrong_magnitude`: current exists but significantly different magnitude
- `execution_partial`: current exists but undersized

**Stage 3: Root Causes (why Stage 1 failed)**
- `below_min_usd`: notional < min_usd
- `no_meta`: symbol not in hl_meta
- `no_mid`: no price data

## Update Frequency Findings

- **Coverage**: 54% of calendar days (64 out of 119 days)
- **Major gaps**: 
  - Aug 10 - Sep 2 (24 days)
  - Nov 5 - Nov 30 (26 days) ⚠️ Cannot diagnose this period
- **Update frequency**: 3-6 times per day when active
- **Peak times**: Midnight UTC (00:00-01:00) and 8am UTC
- **Zero position days**: NONE (all days have 15-34 symbols)

## Implications for diagnose_execution_opus.py

### Current Implementation Issues:
1. ❌ Only compares jianan_v6 → current (skips intermediate 'target' stage)
2. ❌ Cannot distinguish "executor never tried" vs "executor tried but failed"
3. ❌ ADA example shows we can have opposite signs between target and current!

### What We Should Do:
1. ✅ Load BOTH target and current from live_positions
2. ✅ Three-way comparison to isolate where failure occurred
3. ✅ Report separately:
   - Model→Target failures (planner/min constraints)
   - Target→Current failures (execution/exchange issues)

### Example Output:
```
Stage 1 (Model→Target) Failures:
  - 500 symbols: below_min_usd
  - 50 symbols: no_meta
  - 200 symbols: model_not_sent (mystery)

Stage 2 (Target→Current) Failures:
  - 30 symbols: execution_wrong_sign (like ADA)
  - 100 symbols: execution_wrong_magnitude
  - 20 symbols: target_not_achieved
```

This gives us MUCH better insight into WHERE the PnL errors come from!
