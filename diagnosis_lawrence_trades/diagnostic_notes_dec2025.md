## Diagnostic notes: Dec 2025 tracking error

### 0. General workflow for daily diagnostics

When debugging tracking error for a given day **D** (holdings date):

1. **Ensure hourly timeline is up to date**
   - Run:
     ```bash
     python3 scripts/diagnosis_lawrence_trades/hourly_timeline_from_trades.py
     ```
   - This:
     - Rebuilds hourly positions from `maicro_monitors.trades` (cumulative `pos_units` per hour per symbol),
     - Joins Binance 1h prices and account equity,
     - Aligns Jianan model weights from `maicro_logs.positions_jianan_v6` under T+2 (and T+1),
     - Upserts into `maicro_tmp.hourly_timeline_lawrence`.

2. **Pick a snapshot time for the day**
   - The rebalance trades are all between **00:00 and ~00:02 UTC**.
   - After that, the book is effectively constant in `pos_units` for the rest of the day.
   - For diagnostics, it is convenient to look at a single snapshot per day, e.g.:
     - `D 02:00:00` UTC (well after the rebalance, but still that same holdings date).

3. **Pull the hourly snapshot with targets**
   - Example (for day `D = 2025-12-05`):
     ```sql
     SELECT
         ts_hour,
         sym,
         pos_units,
         bn_px,
         equity_usd,
         weight_t2,
         weight_t1
     FROM maicro_tmp.hourly_timeline_lawrence
     WHERE ts_hour = toDateTime('2025-12-05 02:00:00')
     ORDER BY sym;
     ```
   - Compute:
     - `actual_weight  = pos_units * bn_px / equity_usd`
     - `err_t2 = actual_weight - weight_t2`
     - `err_t1 = actual_weight - weight_t1`
   - Use thresholds:
     - `W_EPS  = 0.001` (0.1% weight)
     - `MAG_EPS = 0.02` (2% weight difference)

4. **Classify each (sym, snapshot) into categories**
   - Using T+2 (weight_t2) as the reference:
     - `MATCHED_ZERO`:
       - `|weight_t2| < W_EPS` and `|actual_weight| < W_EPS`.
     - `MISSING_POSITION`:
       - `|weight_t2| ≥ W_EPS` and `|actual_weight| < W_EPS`.
     - `EXTRA_POSITION`:
       - `|weight_t2| < W_EPS` and `|actual_weight| ≥ W_EPS`.
     - `WRONG_DIRECTION`:
       - `|weight_t2| ≥ W_EPS`, `|actual_weight| ≥ W_EPS`, but `sign(actual_weight) ≠ sign(weight_t2)`.
     - `MAGNITUDE_ERROR`:
       - `|weight_t2| ≥ W_EPS`, `|actual_weight| ≥ W_EPS`, same sign,
       - and `|actual_weight - weight_t2| > MAG_EPS`.
     - `MATCHED`:
       - Non-zero weights, same sign, `|actual_weight - weight_t2| ≤ MAG_EPS`.

5. **Attach “flip” attribution (timing error)**
   - Define:
     - `flip_explained = (weight_t1 is not null) and (|err_t1| < |err_t2|)`.
   - This marks rows where the realized position is closer to the T+1 target than to T+2, i.e. likely a 1‑day timing issue rather than pure sizing.

6. **Summarize**
   - For a given day:
     - Count rows per category (`MISSING_POSITION`, `EXTRA_POSITION`, etc.).
     - Aggregate squared error `err_t2^2` by category to see which buckets drive TE.
     - Within each category, compute share of TE from `flip_explained` rows.
   - This gives a clean taxonomy: “how many target positions”, “how many actual positions”, “how much TE from missing vs extra vs wrong‑direction vs magnitude”, and “how much is due to flips”.

The small Python helper used in December 2025 to do this per day is essentially:
  - load from `maicro_tmp.hourly_timeline_lawrence` for the day,
  - filter to snapshot time,
  - compute `actual_weight`,
  - assign categories and print out missing/extra/mis-sized positions.

---

### 1. Dec 5, 2025 (baseline good day)

**Trading pattern**
  - All trades on 2025-12-05 happen between:
    - `00:00:50` and `00:02:10` UTC (35 trades across 31 symbols).
  - No trades after ~00:02.
  - In `maicro_tmp.hourly_timeline_lawrence`, `pos_units` is the same for 00:00, 01:00, 02:00, … i.e. the book is constant after the rebalance.

**Snapshot diagnostics (02:00 UTC)**
  - Total symbols in snapshot: 62.
  - Categories:
    - `MISSING_POSITION`: 2
    - `EXTRA_POSITION`:   0
    - `WRONG_DIRECTION`:  0
    - `MAGNITUDE_ERROR`:  ~30 (but all small, within a few bp of target).
  - The two missing positions:
    - `APT`:
      - `weight_t2 ≈ -0.00365` (small short), `actual_weight ≈ 0`.
    - `HBAR`:
      - `weight_t2 ≈ -0.00200` (tiny short), `actual_weight = 0`.
  - Top long/short names (TAO, BTC, ETH, LINK, BNB, ASTER, AVAX, NEAR, LTC, AAVE, ADA, PAXG, SOL, PUMP, TRX, XPL, BCH, TURBO, COMP, TNSR, WIF, PENGU, etc.) are:
    - In the correct direction.
    - Very close in magnitude: `|actual - target|` typically < 0.002 in weight space.

**Summary for Dec 5**
  - Narrative: One rebalance burst just after midnight builds the intended long/short book almost perfectly.
  - Remaining tracking error:
    - Mostly from tiny missing shorts in APT and HBAR, plus small magnitude tweaks across the rest of the book.

---

### 2. Dec 6, 2025

**Snapshot diagnostics (02:00 UTC)**
  - Total symbols: 62.
  - Categories at 02:00:
    - `MISSING_POSITION`: 3
    - `EXTRA_POSITION`:   0
    - `WRONG_DIRECTION`:  0
    - `MAGNITUDE_ERROR`:  31
  - Missing positions (model wants a position, we’re flat):
    - `WLFI`:
      - `weight_t2 ≈ -0.00473`, `actual_weight ≈ 0`.
    - `TRX`:
      - `weight_t2 ≈ -0.00453`, `actual_weight ≈ 0`.
    - `LINK`:
      - `weight_t2 ≈ -0.00119`, `actual_weight ≈ 0`.
  - Magnitude errors (same sign, but >2% off in weight):
    - Largest examples by `|actual - target|`:
      - `PENGU`: target `≈ -0.0428`, actual `≈ -0.0471` (short slightly larger than model).
      - `PAXG`: target `≈ 0.0550`, actual `≈ 0.0591` (long slightly larger).
      - `BTC`: target `≈ 0.0569`, actual `≈ 0.0596`.
      - `BNB`: target `≈ 0.0485`, actual `≈ 0.0510`.
      - `PUMP`: target `≈ -0.0429`, actual `≈ -0.0454`.
      - … and similar ~0.001–0.004 differences for AAVE, SUI, AVAX, SOL, XPL, VIRTUAL, ZEN, LTC, TNSR, BCH, TAO, STRK, ETH, WLD, WIF, etc.

**Story for Dec 6**
  - The core long/short book (big names) is still aligned in direction and roughly in size.
  - However:
    - A couple of shorts (WLFI, TRX) and a small short in LINK are **missing**.
    - Many weights are off by a few bp (0.1–0.4% in absolute weight), which shows up as `MAGNITUDE_ERROR` but still keep the correct sign.
  - There are no extra or wrong‑direction positions; the mis‑tracking is essentially missing tails plus slightly over/under-sized majors.

---

### 3. Dec 7, 2025

**Snapshot diagnostics (02:00 UTC)**
  - Total symbols: 62.
  - Categories at 02:00:
    - `MISSING_POSITION`: 3
    - `EXTRA_POSITION`:   0
    - `WRONG_DIRECTION`:  0
    - `MAGNITUDE_ERROR`:  29
  - Missing positions:
    - `ARB`:
      - `weight_t2 ≈ +0.01679` (model wants a *long*), `actual_weight ≈ 0.00022` (essentially flat).
    - `WLD`:
      - `weight_t2 ≈ -0.00476` (small short), `actual_weight = 0`.
    - `BCH`:
      - `weight_t2 ≈ +0.00104`, `actual_weight ≈ 0`.
  - Magnitude errors:
    - Largest by `|actual - target|`:
      - `AAVE`: target `≈ 0.0518`, actual `≈ 0.0385` (underweight long by ~1.3%).
      - `PUMP`: target `≈ -0.0517`, actual `≈ -0.0479`.
      - `TNSR`: target `≈ -0.0556`, actual `≈ -0.0518`.
      - `AVAX`: target `≈ 0.0449`, actual `≈ 0.0416`.
      - `PAXG`: target `≈ 0.0535`, actual `≈ 0.0511`.
      - `BTC`, `XPL`, `TRX`, `VIRTUAL`, `BNB`, `ENA`, `STRK`, `FET`, `WIF`, `XRP`, `SOL`, `ETH`, `NEAR`, `SUI`, `UNI` all show ~0.0006–0.0033 differences.

**Story for Dec 7**
  - Still no extra or wrong‑sign positions: directionality is correct across the book.
  - But:
    - A moderate long in `ARB` (~1.7% weight) and two tiny positions in `WLD` and `BCH` are missing.
    - Several core names are under-/over-weighted by ~0.1–1.3% compared to target, which drives the day’s **elevated** TE vs the earlier Dec 3–6 days.

---

### 4. High-level takeaways from Dec 5–7

Across these three days:

1. **Timing / sequencing**
   - All trades happen in a 1–2 minute window after 00:00 UTC each day.
   - The hourly timeline shows that the final book is in place by the end of the 00:00–01:00 hour and then held constant.

2. **Directionality**
   - There are essentially no `WRONG_DIRECTION` or `EXTRA_POSITION` symbols in Dec 5–7 snapshots: the big errors are not “long vs short” flips.

3. **Missing trades**
   - Each day has only a **handful** of `MISSING_POSITION` symbols (2–3 per day), typically:
     - small or moderate `weight_t2`, and
     - actual_weight ≈ 0 (we never traded them).

4. **Magnitude / sizing**
   - Dec 5: magnitude errors are tiny (differences within a few bp).
   - Dec 6–7: magnitude deviations of 0.1–1.3% on some names are more common, which explains why TE ticks back up, even though the book is qualitatively aligned.

This document is the “playbook” for reproducing these diagnostics: re-run the hourly timeline, pick a snapshot hour, compute `actual_weight` vs `weight_t2`, classify into categories, and look specifically at the `MISSING_POSITION` and `MAGNITUDE_ERROR` sets to tell the story of each day.
