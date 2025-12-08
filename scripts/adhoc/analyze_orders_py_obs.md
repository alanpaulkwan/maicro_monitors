Ad hoc: `analyze_orders.py` run (default last 2000 orders)
Run on: 2025-12-08 (UTC)

- Returned slice covers 2025-10-04 07:06:25.376 → 2025-12-07 00:02:03.214 (65 calendar days).
- Days with orders: 36; days missing: 29.
- Missing calendar days within the slice:
  - Singles: 2025-10-11, 2025-10-13, 2025-10-22, 2025-10-24, 2025-10-29, 2025-11-02
  - Gap: 2025-11-09 → 2025-12-01 (inclusive)
- Top order-count days (within slice):
  - 2025-11-04: 103
  - 2025-11-01: 98
  - 2025-10-25: 69
  - 2025-10-23: 67
  - 2025-12-06: 66
  - Cluster at 62–63 orders: 2025-10-28, 2025-12-03, 2025-10-08, 2025-12-05, 2025-12-02

Notes
- Query: `SELECT ... FROM maicro_monitors.orders ORDER BY timestamp DESC LIMIT 2000`
- Script: `scripts/adhoc/analyze_orders.py`

Update 2025-12-08 — coverage vs Jianan targets
- Targets use earliest `inserted_at` per (trade_date, symbol) from `maicro_logs.positions_jianan_v6`, weight finite/non-zero, pred_ret finite.
- Per-day target breadth: 32–37 symbols (median 35) in the 2025-10-02 → 2025-12-05 window.
- Execution offset tests (orders table window 2025-10-04 → 2025-12-07):
  - offset +2d: targets 2,280; orders 921; correct side 433; missing 1,359; wrong side 488.
  - offset +1d: targets 2,283; orders 930; correct 475; missing 1,353; wrong 455.
  - offset +0d: targets 2,282; orders 907; correct 542; missing 1,375; wrong 365.
- Missing examples (offset +2d): BTC +0.0852, AAVE +0.0348, ADA +0.0541, ARB +0.0130, DOGE +0.0032 — none had orders on exec date.
- Wrong-side examples (offset +2d): NEAR short but got buy; SOL long but got sell; UNI short but got buy; XRP long but got sell.

Tracking error from “no-attempt” coins
- Using live_trades (dry_run=0): only 2025-09-03 recorded; targets 36, traded 32, missing 4; TE_abs=0.0849 (~9.9% of |weights|). Missing: DOT, PENDLE, TON, XLM.
- Using live_positions (any run_id): 65 days (2025-08-09 → 2025-12-05). Per-day targets ~35; average TE_abs from untried symbols 0.105 (mean), median TE_pct 10.7% of |weights|; worst days: 2025-09-29 (6 symbols missing, TE_abs 0.250), 2025-10-01, 2025-10-04.
- Worst-day missing symbols (2025-09-29): 0G, AVNT, HEMI, LINEA, WLF I, XPL.

Snapshot parity check (targets vs current positions; live_positions latest per date/symbol)
- Targets: earliest (date, symbol) from positions_jianan_v6 with finite/non-zero weight; Current: latest kind='current' per date/symbol.
- Categories (avg per day across 65 days, 2025-08-09 → 2025-12-05; ~36 targets/day):
  - correct_sign: 22.17
  - long_missing: 2.15
  - short_missing: 3.75
  - long_but_short (target long, current short): 3.05
  - short_but_long (target short, current long): 3.58
  - extra_asset (current present, no target): 0.34
- ~61% of targets have the right sign; ~18% are missing entirely; ~18% are polarity-flipped; a few extras exist with no target.

Speculation from code (~/execution/latest/hl_order)
- Exec offset: scheduler uses target_date +1 day 00:00; monitoring assumed +2. Misalignment can mark present orders as “missing” (or vice versa).
- Min-notional gating: planner enforces MIN_NOTIONAL_USD (env) after reserve/leverage; rounding to size_step/min_units + min_usd can drop small names; executor enforces min notional again on the diff.
- Missing mids/meta: symbols without mids or hl_meta get skipped.
- Side checks: orders are deltas vs current; reductions/closures appear as opposite-side trades relative to raw target sign.
- Capital scaling: reserve + leverage rescales weights; tiny rescaled notionals get filtered out.
