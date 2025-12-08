#!/usr/bin/env python3
"""
Pure SQL waterfall diagnosis for Lawrence trades.

Compares target weights (positions_jianan_v6) vs actual positions (live_positions).

Usage:
    python waterfall_sql.py                          # Default: T+2, 2% threshold
    python waterfall_sql.py --offset 1               # Use T+1 instead
    python waterfall_sql.py --split 2025-12-02       # Compare pre/post split date
    python waterfall_sql.py --strict                 # Only use signals available before holdings_date
"""
import argparse
import sys
sys.path.append('/home/apkwan/standalone_git/maicro_monitors')
from modules.clickhouse_client import query_df


def run_waterfall_sql(offset: int, threshold: float, split_date: str = None, strict: bool = False):
    """Run waterfall analysis via pure SQL."""
    
    # Period clause
    if split_date:
        period_clause = f"CASE WHEN s.expected_holdings < '{split_date}' THEN 'PRE' ELSE 'POST' END as period"
    else:
        period_clause = "'ALL' as period"

    # Signals CTE with optional strict inserted_at filter
    if strict:
        signals_cte = f"""
signals AS (
    SELECT 
        date as signal_date,
        date + INTERVAL {offset} DAY as expected_holdings,
        upper(symbol) as symbol,
        weight as target_weight,
        toDate(inserted_at) as inserted_date
    FROM maicro_logs.positions_jianan_v6
    WHERE weight IS NOT NULL AND isFinite(weight) AND weight != 0
      AND pred_ret IS NOT NULL AND isFinite(pred_ret)
)"""
        # In strict mode, we filter signals where inserted_date < expected_holdings
        signals_active_clause = """
signals_active AS (
    SELECT s.signal_date, s.expected_holdings, s.symbol, s.target_weight
    FROM signals s
    WHERE s.expected_holdings IN (SELECT holdings_date FROM active_holdings_dates)
      AND s.inserted_date < s.expected_holdings
    QUALIFY row_number() OVER(PARTITION BY s.expected_holdings, s.symbol ORDER BY s.inserted_date DESC) = 1
)"""
    else:
        signals_cte = f"""
signals AS (
    SELECT 
        date as signal_date,
        date + INTERVAL {offset} DAY as expected_holdings,
        upper(symbol) as symbol,
        weight as target_weight
    FROM (
        SELECT *, row_number() OVER(PARTITION BY date, symbol ORDER BY inserted_at DESC) rn
        FROM maicro_logs.positions_jianan_v6
        WHERE weight IS NOT NULL AND isFinite(weight) AND weight != 0
          AND pred_ret IS NOT NULL AND isFinite(pred_ret)
    )
    WHERE rn = 1
)"""
        signals_active_clause = """
signals_active AS (
    SELECT * FROM signals
    WHERE expected_holdings IN (SELECT holdings_date FROM active_holdings_dates)
)"""

    sql = f"""
WITH 
-- Get latest position per (holdings_date, symbol)
positions AS (
    SELECT 
        toDate(ts) as holdings_date,
        upper(symbol) as symbol, 
        usd / equity_usd as actual_weight,
        dateDiff('day', target_date, toDate(ts)) as actual_lag
    FROM (
        SELECT *, row_number() OVER(PARTITION BY toDate(ts), symbol, kind ORDER BY ts DESC) rn
        FROM maicro_logs.live_positions
        WHERE kind = 'current'
    )
    WHERE rn = 1
),

{signals_cte},

-- Only keep signals for dates where live_positions exists
active_holdings_dates AS (
    SELECT DISTINCT holdings_date FROM positions
),

{signals_active_clause},

-- Left join to check coverage
aligned AS (
    SELECT 
        s.signal_date,
        s.expected_holdings as holdings_date,
        s.symbol,
        s.target_weight,
        p.actual_weight,
        p.actual_lag,
        p.symbol IS NOT NULL AND p.symbol != '' as has_actual,
        {period_clause}
    FROM signals_active s
    LEFT JOIN positions p 
        ON p.holdings_date = s.expected_holdings
        AND p.symbol = s.symbol
)

SELECT
    period,
    count(*) as total_targets,
    countIf(has_actual) as matched,
    countIf(NOT has_actual) as not_executed,
    
    countIf(has_actual AND sign(target_weight) = sign(actual_weight)) as sign_correct,
    countIf(has_actual AND sign(target_weight) != sign(actual_weight)) as sign_wrong,
    
    countIf(has_actual AND sign(target_weight) = sign(actual_weight) AND abs(actual_weight - target_weight) <= {threshold}) as success,
    countIf(has_actual AND sign(target_weight) = sign(actual_weight) AND abs(actual_weight - target_weight) > {threshold}) as over_threshold,
    
    countIf(has_actual AND actual_lag = {offset}) as correct_offset,
    countIf(has_actual AND actual_lag != {offset}) as wrong_offset
FROM aligned
GROUP BY period
ORDER BY period
"""
    return query_df(sql)


def print_waterfall(df, offset: int, threshold: float, strict: bool):
    """Print waterfall results."""
    mode = "STRICT" if strict else "NORMAL"
    print(f"\n{'='*80}")
    print(f"WATERFALL (T+{offset}, {threshold*100:.0f}% threshold) - {mode} MODE")
    print(f"{'='*80}")
    
    for _, row in df.iterrows():
        period = row['period']
        total = row['total_targets']
        matched = row['matched']
        not_exec = row['not_executed']
        sign_ok = row['sign_correct']
        sign_wrong = row['sign_wrong']
        success = row['success']
        over = row['over_threshold']
        t2_ok = row['correct_offset']
        t2_wrong = row['wrong_offset']
        
        print(f"\n{period} ({total} targets):")
        print(f"  │")
        print(f"  ▼ -{not_exec} not executed ({100*not_exec/total:.1f}%)")
        print(f"  {matched} Matched ({100*matched/total:.1f}%)")
        print(f"  │")
        if matched > 0:
            print(f"  ▼ -{sign_wrong} wrong sign ({100*sign_wrong/matched:.1f}%)")
            print(f"  {sign_ok} Sign correct ({100*sign_ok/matched:.1f}%)")
            print(f"  │")
            sign_ok_safe = sign_ok if sign_ok > 0 else 1
            print(f"  ▼ -{over} over {threshold*100:.0f}% ({100*over/sign_ok_safe:.1f}%)")
            print(f"  {success} SUCCESS ({100*success/total:.1f}% end-to-end)")
            print(f"  ")
            print(f"  Offset: T+{offset}={t2_ok} ({100*t2_ok/matched:.1f}%), wrong={t2_wrong} ({100*t2_wrong/matched:.1f}%)")


def print_side_by_side(df, offset: int, threshold: float):
    """Print PRE vs POST comparison."""
    pre = df[df['period'] == 'PRE'].iloc[0] if 'PRE' in df['period'].values else None
    post = df[df['period'] == 'POST'].iloc[0] if 'POST' in df['period'].values else None
    
    if pre is None or post is None:
        print("Need both PRE and POST periods for comparison")
        return
    
    def pct(n, d): return f"{100*n/d:.1f}%" if d > 0 else "N/A"
    
    print(f"""
  ┌────────────────────────────────────┐    ┌────────────────────────────────────┐
  │           PRE-FIX                  │    │           POST-FIX                 │
  ├────────────────────────────────────┤    ├────────────────────────────────────┤
  │  {pre['total_targets']:4d} Total                       │    │  {post['total_targets']:4d} Total                       │
  │                 │                  │    │                 │                  │
  │▼ -{pre['not_executed']:4d} not executed ({pct(pre['not_executed'], pre['total_targets']):>5})     │    │▼ -{post['not_executed']:4d} not executed ({pct(post['not_executed'], post['total_targets']):>5})     │
  │  {pre['matched']:4d} Matched ({pct(pre['matched'], pre['total_targets']):>5})            │    │  {post['matched']:4d} Matched ({pct(post['matched'], post['total_targets']):>5})            │
  │                 │                  │    │                 │                  │
  │▼ -{pre['sign_wrong']:4d} wrong sign ({pct(pre['sign_wrong'], pre['matched']):>5})        │    │▼ -{post['sign_wrong']:4d} wrong sign ({pct(post['sign_wrong'], post['matched']):>5})        │
  │  {pre['sign_correct']:4d} Sign OK ({pct(pre['sign_correct'], pre['matched']):>5})            │    │  {post['sign_correct']:4d} Sign OK ({pct(post['sign_correct'], post['matched']):>5})            │
  │                 │                  │    │                 │                  │
  │▼ -{pre['over_threshold']:4d} over {threshold*100:.0f}% ({pct(pre['over_threshold'], pre['sign_correct']):>5})          │    │▼ -{post['over_threshold']:4d} over {threshold*100:.0f}% ({pct(post['over_threshold'], post['sign_correct']):>5})          │
  │  {pre['success']:4d} SUCCESS ({pct(pre['success'], pre['total_targets']):>5})            │    │  {post['success']:4d} SUCCESS ({pct(post['success'], post['total_targets']):>5})            │
  ├────────────────────────────────────┤    ├────────────────────────────────────┤
  │ T+{offset}={pre['correct_offset']:4d}  wrong={pre['wrong_offset']:4d}             │    │ T+{offset}={post['correct_offset']:4d}  wrong={post['wrong_offset']:4d}              │
  └────────────────────────────────────┘    └────────────────────────────────────┘
""")
    
    # Summary metrics
    print(f"{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"                        PRE         POST        CHANGE")
    print(f"Coverage:           {pct(pre['matched'], pre['total_targets']):>8}     {pct(post['matched'], post['total_targets']):>8}     {100*post['matched']/post['total_targets'] - 100*pre['matched']/pre['total_targets']:+.1f}%")
    print(f"Sign correct:       {pct(pre['sign_correct'], pre['matched']):>8}     {pct(post['sign_correct'], post['matched']):>8}     {100*post['sign_correct']/post['matched'] - 100*pre['sign_correct']/pre['matched']:+.1f}%")
    print(f"Success:            {pct(pre['success'], pre['total_targets']):>8}     {pct(post['success'], post['total_targets']):>8}     {100*post['success']/post['total_targets'] - 100*pre['success']/pre['total_targets']:+.1f}%")
    print(f"Correct offset:     {pct(pre['correct_offset'], pre['matched']):>8}     {pct(post['correct_offset'], post['matched']):>8}     {100*post['correct_offset']/post['matched'] - 100*pre['correct_offset']/pre['matched']:+.1f}%")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--offset", type=int, default=2, help="Offset in days (default: 2)")
    parser.add_argument("--threshold", type=float, default=0.02, help="Weight threshold (default: 0.02)")
    parser.add_argument("--split", help="Split date for PRE/POST comparison (YYYY-MM-DD)")
    parser.add_argument("--strict", action="store_true", help="Only use signals available before holdings_date")
    args = parser.parse_args()
    
    df = run_waterfall_sql(args.offset, args.threshold, args.split, args.strict)
    
    if args.split:
        print_side_by_side(df, args.offset, args.threshold)
    else:
        print_waterfall(df, args.offset, args.threshold, args.strict)


if __name__ == "__main__":
    main()
