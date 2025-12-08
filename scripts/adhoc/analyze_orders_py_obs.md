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
