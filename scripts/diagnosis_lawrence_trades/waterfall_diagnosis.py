#!/usr/bin/env python3
"""
Waterfall Breakdown: Lawrence Execution Diagnosis

Parameterized script to analyze execution quality with configurable:
- Date range (start_date, end_date)
- Weight threshold for "big gap" (default 2%)

The waterfall includes OFFSET ATTRIBUTION:
- Compares actual execution against CORRECT offset (T+2)
- Shows how much error is due to using WRONG offset (T+1)

Usage:
    python waterfall_diagnosis.py
    python waterfall_diagnosis.py --start_date 2025-12-01 --end_date 2025-12-31
    python waterfall_diagnosis.py --threshold 0.01  # 1% threshold
"""

import os
import sys
import argparse
from datetime import timedelta, date

import pandas as pd
import numpy as np

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(REPO_ROOT)
from modules.clickhouse_client import query_df  # type: ignore

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))


def load_signals_with_inserted_at(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Load target weights from positions_jianan_v6 WITH inserted_at timestamp.
    Returns all records (not deduplicated) so we can filter by inserted_at later.
    """
    where = "1=1"
    params = {}
    if start_date:
        where += " AND date >= %(start)s"
        params["start"] = start_date
    if end_date:
        where += " AND date <= %(end)s"
        params["end"] = end_date
    
    sql = f"""
        SELECT date, symbol, weight, inserted_at
        FROM maicro_logs.positions_jianan_v6
        WHERE {where}
          AND weight IS NOT NULL AND isFinite(weight) AND weight != 0
          AND pred_ret IS NOT NULL AND isFinite(pred_ret)
    """
    df = query_df(sql, params=params)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    return df


def load_signals(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Load target weights from positions_jianan_v6 (deduplicated, latest per date/symbol)."""
    where = "1=1"
    params = {}
    if start_date:
        where += " AND date >= %(start)s"
        params["start"] = start_date
    if end_date:
        where += " AND date <= %(end)s"
        params["end"] = end_date
    
    sql = f"""
        SELECT date, symbol, weight
        FROM (
            SELECT date, symbol, weight, inserted_at
            FROM maicro_logs.positions_jianan_v6
            WHERE {where}
              AND weight IS NOT NULL AND isFinite(weight) AND weight != 0
              AND pred_ret IS NOT NULL AND isFinite(pred_ret)
            ORDER BY date, symbol, inserted_at
            LIMIT 1 BY date, symbol
        )
    """
    df = query_df(sql, params=params)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].str.upper().str.strip()
    return df


def load_positions(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Load actual positions from live_positions (kind='current').
    Uses ts (holdings date) as anchor.
    """
    where = "kind = 'current'"
    params = {}
    if start_date:
        where += " AND toDate(ts) >= %(start)s"
        params["start"] = start_date
    if end_date:
        where += " AND toDate(ts) <= %(end)s"
        params["end"] = end_date
    
    sql = f"""
        SELECT 
            ts,
            toDate(ts) as holdings_date,
            target_date,
            symbol, 
            usd, 
            equity_usd
        FROM (
            SELECT *, row_number() OVER(PARTITION BY toDate(ts), symbol, kind ORDER BY ts DESC) rn
            FROM maicro_logs.live_positions
            WHERE {where}
        )
        WHERE rn = 1
    """
    df = query_df(sql, params=params)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    df["holdings_date"] = pd.to_datetime(df["holdings_date"]).dt.date
    df["target_date"] = pd.to_datetime(df["target_date"]).dt.date
    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["weight"] = df["usd"] / df["equity_usd"]
    # Compute actual lag (what offset Lawrence actually used)
    df["actual_lag"] = df.apply(lambda r: (r["holdings_date"] - r["target_date"]).days, axis=1)
    return df


def run_waterfall(signals: pd.DataFrame, positions: pd.DataFrame, 
                  offset: int, threshold: float, label: str,
                  use_inserted_at_filter: bool = False,
                  signals_with_ts: pd.DataFrame = None) -> dict:
    """
    Run waterfall analysis for a specific offset.
    
    Parameters:
    -----------
    signals : DataFrame with (date, symbol, weight) - deduplicated signals
    positions : DataFrame with (holdings_date, symbol, weight, ts) from live_positions
    offset : int, number of days to subtract from holdings_date to get signal_date
    threshold : float, weight difference threshold for "big gap"
    label : str, label for this analysis
    use_inserted_at_filter : bool, if True, only use signals where inserted_at < holdings_date
    signals_with_ts : DataFrame with (date, symbol, weight, inserted_at) - raw signals with timestamps
    
    Returns dict with all stage counts and the merged dataframe.
    """
    if positions.empty:
        return None
    
    # Get holdings dates
    holdings_dates = set(positions["holdings_date"].unique())
    
    # Filter signals to dates where we have holdings (shifted by offset)
    signal_dates_needed = set(d - timedelta(days=offset) for d in holdings_dates)
    
    if use_inserted_at_filter and signals_with_ts is not None:
        # For each position's holdings_date, we need to find the latest signal
        # where signal.date = holdings_date - offset AND signal.inserted_at < holdings_date
        
        pos_df = positions.copy()
        pos_df["expected_signal_date"] = pos_df["holdings_date"].apply(
            lambda d: d - timedelta(days=offset)
        )
        
        # Filter signals to relevant dates
        sigs_filtered = signals_with_ts[signals_with_ts["date"].isin(signal_dates_needed)].copy()
        sigs_filtered["inserted_date"] = sigs_filtered["inserted_at"].dt.date
        
        # Get unique (holdings_date, symbol) from positions, merge with signals
        pos_keys = pos_df[["holdings_date", "symbol", "expected_signal_date", "weight", "actual_lag"]].copy()
        pos_keys = pos_keys.rename(columns={"weight": "actual_weight"})
        
        # Merge all possible signals
        merged = pos_keys.merge(
            sigs_filtered[["date", "symbol", "weight", "inserted_date"]],
            left_on=["expected_signal_date", "symbol"],
            right_on=["date", "symbol"],
            how="left"
        )
        
        # Filter: only keep signals where inserted_date < holdings_date
        merged["signal_valid"] = merged.apply(
            lambda r: pd.notna(r["inserted_date"]) and r["inserted_date"] < r["holdings_date"],
            axis=1
        )
        
        # Set weight to NaN for invalid signals
        merged.loc[~merged["signal_valid"], "weight"] = np.nan
        
        # Keep only valid rows (one per holdings_date/symbol)
        merged = merged.drop_duplicates(subset=["holdings_date", "symbol"], keep="first")
        merged["expected_holdings"] = merged["holdings_date"]
        
    else:
        # Original logic without inserted_at filter
        signals_filtered = signals[signals["date"].isin(signal_dates_needed)].copy()
        
        if signals_filtered.empty:
            return None
        
        signals_filtered["expected_holdings"] = signals_filtered["date"].apply(
            lambda d: d + timedelta(days=offset)
        )
        
        # Merge signals with positions
        merged = signals_filtered.merge(
            positions[["holdings_date", "symbol", "weight", "actual_lag"]].rename(
                columns={"weight": "actual_weight"}
            ),
            left_on=["expected_holdings", "symbol"],
            right_on=["holdings_date", "symbol"],
            how="left"
        )
    
    # Stage 1: Coverage
    merged["has_actual"] = merged["actual_weight"].notna()
    merged["actual_weight"] = merged["actual_weight"].fillna(0.0)
    merged["weight"] = merged["weight"].fillna(0.0)
    
    n_total = len(merged)
    n_has_actual = merged["has_actual"].sum()
    n_missing = n_total - n_has_actual
    
    # Stage 2: Direction
    matched = merged[merged["has_actual"]].copy()
    matched["target_sign"] = np.sign(matched["weight"])
    matched["actual_sign"] = np.sign(matched["actual_weight"])
    matched["sign_correct"] = matched["target_sign"] == matched["actual_sign"]
    
    n_matched = len(matched)
    n_sign_correct = matched["sign_correct"].sum()
    n_sign_wrong = n_matched - n_sign_correct
    
    # Stage 3: Magnitude
    correct_sign = matched[matched["sign_correct"]].copy()
    if len(correct_sign) > 0:
        correct_sign["weight_diff"] = correct_sign["actual_weight"] - correct_sign["weight"]
        correct_sign["abs_diff"] = correct_sign["weight_diff"].abs()
        correct_sign["within_threshold"] = correct_sign["abs_diff"] <= threshold
        n_within = correct_sign["within_threshold"].sum()
    else:
        n_within = 0
    
    n_correct_sign = len(correct_sign)
    n_over = n_correct_sign - n_within
    
    # Offset attribution: how many used wrong offset?
    if n_matched > 0 and "actual_lag" in matched.columns:
        valid_lag = matched["actual_lag"].notna()
        n_wrong_offset = ((matched["actual_lag"] != offset) & valid_lag).sum()
        n_correct_offset = ((matched["actual_lag"] == offset) & valid_lag).sum()
    else:
        n_wrong_offset = 0
        n_correct_offset = 0
    
    return {
        "label": label,
        "offset": offset,
        "threshold": threshold,
        "n_total": n_total,
        "n_has_actual": n_has_actual,
        "n_missing": n_missing,
        "n_matched": n_matched,
        "n_sign_correct": n_sign_correct,
        "n_sign_wrong": n_sign_wrong,
        "n_within": n_within,
        "n_over": n_over,
        "n_wrong_offset": n_wrong_offset,
        "n_correct_offset": n_correct_offset,
        "merged": merged,
        "matched": matched,
        "correct_sign": correct_sign,
    }


def print_waterfall(result: dict, show_details: bool = True):
    """Print waterfall breakdown."""
    if result is None:
        print("No data for this period")
        return
    
    r = result
    threshold_pct = r["threshold"] * 100
    
    print(f"""
{'='*100}
WATERFALL: {r['label']} (Offset=T+{r['offset']}, Threshold={threshold_pct:.0f}%)
{'='*100}

STAGE 0: STARTING UNIVERSE
--------------------------
Total targets (signals for days with live_positions): {r['n_total']}

STAGE 1: COVERAGE (Target → Actual Position Exists?)
----------------------------------------------------
✓ Has actual position:    {r['n_has_actual']:>6} ({100*r['n_has_actual']/r['n_total']:.1f}%)
✗ Missing (not executed): {r['n_missing']:>6} ({100*r['n_missing']/r['n_total']:.1f}%)

STAGE 2: DIRECTION (Sign matches?)
----------------------------------
✓ Sign correct:  {r['n_sign_correct']:>6} ({100*r['n_sign_correct']/r['n_matched']:.1f}% of matched)
✗ Sign wrong:    {r['n_sign_wrong']:>6} ({100*r['n_sign_wrong']/r['n_matched']:.1f}% of matched)

STAGE 3: MAGNITUDE (Weight diff ≤ {threshold_pct:.0f}%?)
-----------------------------------------
✓ Within {threshold_pct:.0f}%:  {r['n_within']:>6} ({100*r['n_within']/max(1,r['n_sign_correct']):.1f}% of sign-correct)
✗ Over {threshold_pct:.0f}%:    {r['n_over']:>6} ({100*r['n_over']/max(1,r['n_sign_correct']):.1f}% of sign-correct)

OFFSET ATTRIBUTION (of {r['n_matched']} matched positions)
---------------------------------------------------------
✓ Used correct offset (T+{r['offset']}): {r['n_correct_offset']:>6} ({100*r['n_correct_offset']/max(1,r['n_matched']):.1f}%)
✗ Used wrong offset:                     {r['n_wrong_offset']:>6} ({100*r['n_wrong_offset']/max(1,r['n_matched']):.1f}%)

{'='*100}
WATERFALL FUNNEL
{'='*100}

  {r['n_total']:>6} Total targets
     │
     ▼ -{r['n_missing']} not executed
  {r['n_has_actual']:>6} Matched ({100*r['n_has_actual']/r['n_total']:.1f}%)
     │
     ▼ -{r['n_sign_wrong']} wrong sign
  {r['n_sign_correct']:>6} Sign correct ({100*r['n_sign_correct']/max(1,r['n_matched']):.1f}%)
     │
     ▼ -{r['n_over']} over {threshold_pct:.0f}%
  {r['n_within']:>6} SUCCESS ({100*r['n_within']/r['n_total']:.1f}% end-to-end)

{'='*100}
LOSS ATTRIBUTION
{'='*100}
Total losses: {r['n_total'] - r['n_within']}
""")
    
    total_loss = r['n_total'] - r['n_within']
    if total_loss > 0:
        print(f"""  Not executed:  {r['n_missing']:>6} ({100*r['n_missing']/total_loss:.1f}% of losses)
  Wrong sign:    {r['n_sign_wrong']:>6} ({100*r['n_sign_wrong']/total_loss:.1f}% of losses)
  Wrong size:    {r['n_over']:>6} ({100*r['n_over']/total_loss:.1f}% of losses)
  
  Of which, due to WRONG OFFSET: ~{r['n_wrong_offset']} positions used T+1 instead of T+2
""")
    
    if show_details and r['n_sign_wrong'] > 0:
        print("\nTop symbols with WRONG SIGN:")
        wrong = r['matched'][~r['matched']['sign_correct']]
        print(wrong["symbol"].value_counts().head(10).to_string())
    
    if show_details and r['n_sign_correct'] > 0:
        print(f"\nMAGNITUDE BREAKDOWN (of {r['n_sign_correct']} sign-correct):")
        cs = r['correct_sign']
        for lo, hi, lbl in [(0, 0.02, "≤2%"), (0.02, 0.05, "2-5%"), (0.05, 0.10, "5-10%"), (0.10, 1.0, ">10%")]:
            if lo == 0:
                cnt = (cs["abs_diff"] <= hi).sum()
            else:
                cnt = ((cs["abs_diff"] > lo) & (cs["abs_diff"] <= hi)).sum()
            print(f"  {lbl}: {cnt} ({100*cnt/r['n_sign_correct']:.1f}%)")


def compare_offsets(signals: pd.DataFrame, positions: pd.DataFrame, threshold: float):
    """Compare T+1 vs T+2 alignment to show cost of offset error."""
    
    r_t2 = run_waterfall(signals, positions, offset=2, threshold=threshold, label="CORRECT (T+2)")
    r_t1 = run_waterfall(signals, positions, offset=1, threshold=threshold, label="WRONG (T+1)")
    
    if r_t2 is None or r_t1 is None:
        print("Insufficient data for offset comparison")
        return
    
    print(f"""
{'='*100}
OFFSET COMPARISON: T+2 (correct) vs T+1 (wrong)
{'='*100}

                              T+2 (correct)    T+1 (wrong)     Difference
                              -------------    -----------     ----------
Targets:                      {r_t2['n_total']:>12}   {r_t1['n_total']:>12}
Matched:                      {r_t2['n_has_actual']:>12}   {r_t1['n_has_actual']:>12}     {r_t1['n_has_actual']-r_t2['n_has_actual']:>+10}
Sign correct:                 {r_t2['n_sign_correct']:>12}   {r_t1['n_sign_correct']:>12}     {r_t1['n_sign_correct']-r_t2['n_sign_correct']:>+10}
Within {threshold*100:.0f}%:                   {r_t2['n_within']:>12}   {r_t1['n_within']:>12}     {r_t1['n_within']-r_t2['n_within']:>+10}

Sign correct %:               {100*r_t2['n_sign_correct']/max(1,r_t2['n_matched']):>11.1f}%  {100*r_t1['n_sign_correct']/max(1,r_t1['n_matched']):>11.1f}%    {100*(r_t1['n_sign_correct']/max(1,r_t1['n_matched'])-r_t2['n_sign_correct']/max(1,r_t2['n_matched'])):>+9.1f}%
Within {threshold*100:.0f}% of sign-correct:  {100*r_t2['n_within']/max(1,r_t2['n_sign_correct']):>11.1f}%  {100*r_t1['n_within']/max(1,r_t1['n_sign_correct']):>11.1f}%    {100*(r_t1['n_within']/max(1,r_t1['n_sign_correct'])-r_t2['n_within']/max(1,r_t2['n_sign_correct'])):>+9.1f}%
End-to-end success:           {100*r_t2['n_within']/r_t2['n_total']:>11.1f}%  {100*r_t1['n_within']/r_t1['n_total']:>11.1f}%    {100*(r_t1['n_within']/r_t1['n_total']-r_t2['n_within']/r_t2['n_total']):>+9.1f}%
""")
    
    # Compute mean tracking error
    if len(r_t2['correct_sign']) > 0 and len(r_t1['correct_sign']) > 0:
        te_t2 = r_t2['correct_sign']['abs_diff'].mean()
        te_t1 = r_t1['correct_sign']['abs_diff'].mean()
        print(f"""Mean tracking error:          {te_t2:>11.4f}   {te_t1:>11.4f}     {te_t1-te_t2:>+9.4f}
""")
    
    return r_t2, r_t1


def print_waterfall_ascii(label: str, r: dict, width: int = 40) -> list:
    """Generate ASCII art waterfall box."""
    if r is None:
        return [f"No data for {label}"]
    
    s = r
    lines = []
    lines.append(f"┌{'─'*(width-2)}┐")
    lines.append(f"│{label:^{width-2}}│")
    lines.append(f"├{'─'*(width-2)}┤")
    lines.append(f"│{s['n_total']:>6} Total targets{' '*(width-22)}│")
    lines.append(f"│{'│':^{width-2}}│")
    
    miss_pct = 100*s['n_missing']/s['n_total'] if s['n_total'] > 0 else 0
    lines.append(f"│▼ -{s['n_missing']} not executed ({miss_pct:.0f}%){' '*(width-30)}│")
    
    match_pct = 100*s['n_has_actual']/s['n_total'] if s['n_total'] > 0 else 0
    lines.append(f"│{s['n_has_actual']:>6} Matched ({match_pct:.0f}%){' '*(width-24)}│")
    lines.append(f"│{'│':^{width-2}}│")
    
    sign_wrong_pct = 100*s['n_sign_wrong']/s['n_matched'] if s['n_matched'] > 0 else 0
    lines.append(f"│▼ -{s['n_sign_wrong']} wrong sign ({sign_wrong_pct:.0f}%){' '*(width-28)}│")
    
    sign_ok_pct = 100*s['n_sign_correct']/s['n_matched'] if s['n_matched'] > 0 else 0
    lines.append(f"│{s['n_sign_correct']:>6} Sign OK ({sign_ok_pct:.0f}%){' '*(width-22)}│")
    lines.append(f"│{'│':^{width-2}}│")
    
    over_pct = 100*s['n_over']/s['n_sign_correct'] if s['n_sign_correct'] > 0 else 0
    lines.append(f"│▼ -{s['n_over']} over 2% ({over_pct:.0f}%){' '*(width-25)}│")
    
    success_pct = 100*s['n_within']/s['n_total'] if s['n_total'] > 0 else 0
    lines.append(f"│{s['n_within']:>6} SUCCESS ({success_pct:.1f}%){' '*(width-24)}│")
    lines.append(f"├{'─'*(width-2)}┤")
    lines.append(f"│ T+2={s['n_correct_offset']} T+1={s['n_wrong_offset']}{' '*(width-22)}│")
    lines.append(f"└{'─'*(width-2)}┘")
    return lines


def print_side_by_side(label1: str, r1: dict, label2: str, r2: dict):
    """Print two waterfalls side by side."""
    w = 38
    lines1 = print_waterfall_ascii(label1, r1, w)
    lines2 = print_waterfall_ascii(label2, r2, w)
    
    print()
    for l1, l2 in zip(lines1, lines2):
        print(f"  {l1}    {l2}")
    print()
    
    # Summary table
    if r1 and r2:
        print("="*90)
        print("SUMMARY METRICS")
        print("="*90)
        
        def pct(n, d): return 100*n/d if d > 0 else 0
        
        cov1 = pct(r1['n_has_actual'], r1['n_total'])
        cov2 = pct(r2['n_has_actual'], r2['n_total'])
        sign1 = pct(r1['n_sign_correct'], r1['n_matched'])
        sign2 = pct(r2['n_sign_correct'], r2['n_matched'])
        mag1 = pct(r1['n_within'], r1['n_sign_correct'])
        mag2 = pct(r2['n_within'], r2['n_sign_correct'])
        e2e1 = pct(r1['n_within'], r1['n_total'])
        e2e2 = pct(r2['n_within'], r2['n_total'])
        off1 = pct(r1['n_correct_offset'], r1['n_matched'])
        off2 = pct(r2['n_correct_offset'], r2['n_matched'])
        
        print(f"""
                            {label1[:15]:<15} {label2[:15]:<15} CHANGE
                            {'─'*15} {'─'*15} {'─'*10}
Total targets:              {r1['n_total']:>15} {r2['n_total']:>15}
Coverage:                   {cov1:>14.1f}% {cov2:>14.1f}% {cov2-cov1:>+9.1f}%
Sign correct:               {sign1:>14.1f}% {sign2:>14.1f}% {sign2-sign1:>+9.1f}%
Within 2%:                  {mag1:>14.1f}% {mag2:>14.1f}% {mag2-mag1:>+9.1f}%
End-to-end success:         {e2e1:>14.1f}% {e2e2:>14.1f}% {e2e2-e2e1:>+9.1f}%
Correct offset (T+2):       {off1:>14.1f}% {off2:>14.1f}% {off2-off1:>+9.1f}%
""")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--start_date", help="Start date (YYYY-MM-DD) for holdings")
    parser.add_argument("--end_date", help="End date (YYYY-MM-DD) for holdings")
    parser.add_argument("--threshold", type=float, default=0.02,
                       help="Weight threshold for 'big gap' (default: 0.02 = 2%%)")
    parser.add_argument("--offset", type=int, default=2,
                       help="Expected offset: signal_date = holdings_date - offset (default: 2)")
    parser.add_argument("--compare", action="store_true",
                       help="Compare T+1 vs T+2 offsets")
    parser.add_argument("--split", help="Date to split pre/post comparison (YYYY-MM-DD), e.g. 2025-12-02")
    parser.add_argument("--strict", action="store_true",
                       help="Use strict mode: only use signals where inserted_at < holdings_date")
    
    args = parser.parse_args()
    
    print("Loading data...")
    
    # Load positions for the date range
    positions = load_positions(args.start_date, args.end_date)
    if positions.empty:
        print("No positions found for date range")
        return
    
    # Determine signal date range based on positions and offset
    min_holdings = positions["holdings_date"].min()
    max_holdings = positions["holdings_date"].max()
    signal_start = str(min_holdings - timedelta(days=args.offset + 5))  # buffer
    signal_end = str(max_holdings)
    
    signals = load_signals(signal_start, signal_end)
    
    # Load signals with inserted_at if strict mode
    signals_with_ts = None
    if args.strict:
        signals_with_ts = load_signals_with_inserted_at(signal_start, signal_end)
        print(f"Strict mode: using inserted_at filter (signals must be inserted before holdings_date)")
    
    print(f"Loaded {len(signals)} signals, {len(positions)} positions")
    print(f"Holdings date range: {min_holdings} → {max_holdings}")
    
    # Split comparison mode
    if args.split:
        split_date = pd.to_datetime(args.split).date()
        print(f"\n{'='*90}")
        print(f"WATERFALL COMPARISON: PRE vs POST {split_date}")
        print(f"{'='*90}")
        
        pos_pre = positions[positions["holdings_date"] < split_date]
        pos_post = positions[positions["holdings_date"] >= split_date]
        
        r_pre = run_waterfall(signals, pos_pre, args.offset, args.threshold, f"PRE ({split_date})",
                             use_inserted_at_filter=args.strict, signals_with_ts=signals_with_ts)
        r_post = run_waterfall(signals, pos_post, args.offset, args.threshold, f"POST ({split_date})",
                              use_inserted_at_filter=args.strict, signals_with_ts=signals_with_ts)
        
        print_side_by_side(
            f"PRE-FIX (before {split_date})",
            r_pre,
            f"POST-FIX ({split_date}+)",
            r_post
        )
        return
    
    if args.compare:
        compare_offsets(signals, positions, args.threshold)
    
    # Main waterfall with specified offset
    result = run_waterfall(signals, positions, args.offset, args.threshold, 
                          f"Analysis (T+{args.offset})",
                          use_inserted_at_filter=args.strict, signals_with_ts=signals_with_ts)
    print_waterfall(result)
    
    # Save detailed output
    if result:
        output_file = os.path.join(OUTPUT_DIR, "waterfall_detailed.csv")
        result["merged"].to_csv(output_file, index=False)
        print(f"\nDetailed data saved to: {output_file}")


if __name__ == "__main__":
    main()
