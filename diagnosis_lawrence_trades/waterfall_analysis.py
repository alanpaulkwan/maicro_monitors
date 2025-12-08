"""
Waterfall Analysis: Diagnose PnL errors from Lawrence's trades
================================================================
This script analyzes the gap between target positions (from Jianan's model)
and actual positions (what Lawrence traded).

Key findings:
1. Lawrence used T-1 offset (signal at T, trade at T+1) instead of correct T-2
2. Many positions were skipped due to being below minimum notional
3. Some wrong-sign positions are explained by the offset error

Usage:
    python waterfall_analysis.py [--start-date YYYY-MM-DD]
"""

import argparse
import clickhouse_connect
import pandas as pd
from datetime import date

# Connection config
CH_CONFIG = {
    'host': 'chenlin04.fbe.hku.hk',
    'port': 8123,
    'user': 'maicrobot',
    'password': 'iamsentient',
    'database': 'maicro_logs'
}


def get_client():
    return clickhouse_connect.get_client(**CH_CONFIG)


def run_waterfall(client, start_date: str):
    """Main waterfall breakdown with T-2 offset (correct alignment)."""
    sql = f"""
    WITH 
    actual AS (
        SELECT 
            toDate(ts) as holdings_date,
            any(target_date) as signal_date_used,
            upper(symbol) as sym,
            sum(usd) as actual_usd,
            any(equity_usd) as portfolio_value
        FROM maicro_logs.live_positions
        WHERE kind = 'current'
        AND toDate(ts) >= '{start_date}'
        GROUP BY toDate(ts), upper(symbol)
    ),
    targets_deduped AS (
        SELECT 
            toDate(date) as signal_date,
            upper(symbol) as sym,
            argMax(weight, inserted_at) as target_weight
        FROM maicro_logs.positions_jianan_v6
        GROUP BY toDate(date), upper(symbol)
    ),
    targets_t2 AS (
        SELECT signal_date, signal_date + 2 as holdings_date, sym, target_weight
        FROM targets_deduped
        WHERE signal_date + 2 >= '{start_date}'
    ),
    aligned AS (
        SELECT 
            coalesce(a.holdings_date, t.holdings_date) as holdings_date,
            coalesce(a.sym, t.sym) as sym,
            t.target_weight,
            a.actual_usd,
            a.portfolio_value,
            a.signal_date_used
        FROM actual a
        FULL OUTER JOIN targets_t2 t 
            ON a.holdings_date = t.holdings_date AND a.sym = t.sym
    ),
    with_weights AS (
        SELECT 
            holdings_date,
            sym,
            target_weight,
            actual_usd,
            portfolio_value,
            signal_date_used,
            CASE 
                WHEN portfolio_value > 0 THEN actual_usd / portfolio_value
                ELSE 0
            END as actual_weight
        FROM aligned
    ),
    classified AS (
        SELECT 
            holdings_date,
            sym,
            target_weight,
            actual_weight,
            CASE
                WHEN target_weight IS NULL AND abs(coalesce(actual_weight,0)) > 0.001 THEN 'EXTRA_POSITION'
                WHEN target_weight IS NOT NULL AND abs(coalesce(target_weight,0)) > 0.001 AND abs(coalesce(actual_weight, 0)) < 0.001 THEN 'MISSING_POSITION'
                WHEN target_weight IS NOT NULL AND actual_weight IS NOT NULL AND abs(actual_weight) >= 0.001 AND sign(target_weight) != sign(actual_weight) THEN 'WRONG_SIGN'
                WHEN target_weight IS NOT NULL AND actual_weight IS NOT NULL AND abs(target_weight - actual_weight) > 0.02 THEN 'MAGNITUDE_ERROR'
                WHEN target_weight IS NOT NULL AND actual_weight IS NOT NULL THEN 'MATCHED'
                ELSE 'OTHER'
            END as category
        FROM with_weights
        WHERE abs(coalesce(target_weight, 0)) > 0.001 OR abs(coalesce(actual_weight, 0)) > 0.001
    )
    SELECT 
        category,
        count(*) as count,
        round(count(*) * 100.0 / sum(count(*)) OVER (), 2) as pct
    FROM classified
    GROUP BY category
    ORDER BY count DESC
    """
    result = client.query(sql)
    return pd.DataFrame(result.result_rows, columns=['Category', 'Count', 'Pct'])


def run_missing_breakdown(client, start_date: str):
    """Break down missing positions by min notional threshold."""
    sql = f"""
    WITH 
    actual AS (
        SELECT 
            toDate(ts) as holdings_date,
            upper(symbol) as sym,
            sum(usd) as actual_usd,
            any(equity_usd) as portfolio_value
        FROM maicro_logs.live_positions
        WHERE kind = 'current' AND toDate(ts) >= '{start_date}'
        GROUP BY toDate(ts), upper(symbol)
    ),
    targets_deduped AS (
        SELECT 
            toDate(date) as signal_date,
            upper(symbol) as sym,
            argMax(weight, inserted_at) as target_weight
        FROM maicro_logs.positions_jianan_v6
        GROUP BY toDate(date), upper(symbol)
    ),
    targets_t2 AS (
        SELECT signal_date, signal_date + 2 as holdings_date, sym, target_weight
        FROM targets_deduped
        WHERE signal_date + 2 >= '{start_date}'
    ),
    trading_days AS (
        SELECT DISTINCT toDate(ts) as holdings_date
        FROM maicro_logs.live_positions
        WHERE kind = 'current' AND toDate(ts) >= '{start_date}'
    ),
    missing_with_meta AS (
        SELECT 
            t.holdings_date,
            t.sym,
            t.target_weight,
            coalesce(a.portfolio_value, 2000) as pv,
            h.min_usd,
            abs(t.target_weight) * coalesce(a.portfolio_value, 2000) as target_notional
        FROM targets_t2 t
        INNER JOIN trading_days td ON t.holdings_date = td.holdings_date
        LEFT JOIN actual a ON t.holdings_date = a.holdings_date AND t.sym = a.sym
        LEFT JOIN maicro_logs.hl_meta h ON t.sym = upper(h.symbol)
        WHERE (a.actual_usd IS NULL OR abs(a.actual_usd) < 1)
        AND abs(t.target_weight) > 0.001
    )
    SELECT 
        CASE 
            WHEN target_notional < coalesce(min_usd, 10) THEN 'BELOW_MIN_NOTIONAL'
            ELSE 'ABOVE_MIN_NOTIONAL'
        END as reason,
        count(*) as cnt,
        round(count(*) * 100.0 / sum(count(*)) OVER (), 2) as pct
    FROM missing_with_meta
    GROUP BY reason
    ORDER BY cnt DESC
    """
    result = client.query(sql)
    return pd.DataFrame(result.result_rows, columns=['Reason', 'Count', 'Pct'])


def run_wrong_sign_breakdown(client, start_date: str):
    """Break down wrong-sign positions by whether they match T-1 or T-3 offsets."""
    sql = f"""
    WITH 
    actual AS (
        SELECT 
            toDate(ts) as holdings_date,
            upper(symbol) as sym,
            sum(usd) as actual_usd,
            any(equity_usd) as portfolio_value
        FROM maicro_logs.live_positions
        WHERE kind = 'current' AND toDate(ts) >= '{start_date}'
        GROUP BY toDate(ts), upper(symbol)
    ),
    targets_deduped AS (
        SELECT 
            toDate(date) as signal_date,
            upper(symbol) as sym,
            argMax(weight, inserted_at) as target_weight
        FROM maicro_logs.positions_jianan_v6
        GROUP BY toDate(date), upper(symbol)
    ),
    wrong_sign_analysis AS (
        SELECT 
            a.holdings_date,
            a.sym,
            t2.target_weight as target_t2,
            t1.target_weight as target_t1,
            t3.target_weight as target_t3,
            a.actual_usd / nullIf(a.portfolio_value, 0) as actual_weight
        FROM actual a
        JOIN targets_deduped t2 ON a.holdings_date = t2.signal_date + 2 AND a.sym = t2.sym
        LEFT JOIN targets_deduped t1 ON a.holdings_date = t1.signal_date + 1 AND a.sym = t1.sym
        LEFT JOIN targets_deduped t3 ON a.holdings_date = t3.signal_date + 3 AND a.sym = t3.sym
        WHERE sign(t2.target_weight) != sign(a.actual_usd)
        AND abs(a.actual_usd) > 1
        AND abs(t2.target_weight) > 0.001
    )
    SELECT 
        CASE 
            WHEN target_t1 IS NOT NULL AND sign(target_t1) = sign(actual_weight) THEN 'DUE_TO_T1_OFFSET'
            WHEN target_t3 IS NOT NULL AND sign(target_t3) = sign(actual_weight) THEN 'DUE_TO_T3_TIMING'
            ELSE 'UNEXPLAINED'
        END as explanation,
        count(*) as cnt,
        round(count(*) * 100.0 / sum(count(*)) OVER (), 2) as pct
    FROM wrong_sign_analysis
    GROUP BY explanation
    ORDER BY cnt DESC
    """
    result = client.query(sql)
    return pd.DataFrame(result.result_rows, columns=['Explanation', 'Count', 'Pct'])


def run_trading_days_count(client, start_date: str):
    """Count trading days in period."""
    sql = f"""
    SELECT count(DISTINCT toDate(ts)) 
    FROM maicro_logs.live_positions 
    WHERE kind='current' AND toDate(ts) >= '{start_date}'
    """
    result = client.query(sql)
    return result.result_rows[0][0]


def print_waterfall_ascii(waterfall_df):
    """Print ASCII waterfall chart."""
    total = waterfall_df['Count'].sum()
    print("\n" + "=" * 60)
    for _, row in waterfall_df.iterrows():
        bar_len = int(row['Pct'] / 2)
        bar = "█" * bar_len
        print(f"{row['Category']:20s} │ {bar:30s} {row['Count']:5d} ({row['Pct']:5.1f}%)")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Waterfall analysis of trading errors')
    parser.add_argument('--start-date', type=str, default='2025-10-15',
                        help='Start date for analysis (YYYY-MM-DD)')
    args = parser.parse_args()

    client = get_client()
    
    # Run for two periods
    periods = [
        ('Since Oct 15 2025', '2025-10-15'),
        ('Since Dec 2 2025', '2025-12-02'),
    ]
    
    # If custom start date provided, use only that
    if args.start_date not in ['2025-10-15', '2025-12-02']:
        periods = [(f'Since {args.start_date}', args.start_date)]

    for period_name, start_date in periods:
        print(f"\n{'=' * 70}")
        print(f" {period_name}")
        print('=' * 70)
        
        trading_days = run_trading_days_count(client, start_date)
        print(f"\nTrading days with positions: {trading_days}")
        
        # Main waterfall
        print("\n▼ WATERFALL BREAKDOWN (T-2 offset = correct alignment)")
        waterfall = run_waterfall(client, start_date)
        print_waterfall_ascii(waterfall)
        
        # Missing breakdown
        print("\n▼ MISSING POSITIONS: Why were they skipped?")
        missing = run_missing_breakdown(client, start_date)
        print(missing.to_string(index=False))
        
        # Wrong sign breakdown
        print("\n▼ WRONG SIGN: Why is the direction wrong?")
        wrong_sign = run_wrong_sign_breakdown(client, start_date)
        print(wrong_sign.to_string(index=False))
        
    print("\n" + "=" * 70)
    print("SUMMARY:")
    print("- MISSING_POSITION: Model wanted a position but we didn't trade")
    print("  -> BELOW_MIN_NOTIONAL: Target notional < exchange minimum")
    print("  -> ABOVE_MIN_NOTIONAL: Other reason (execution failure?)")
    print("- WRONG_SIGN: We're long when model says short (or vice versa)")
    print("  -> DUE_TO_T1_OFFSET: Lawrence used T-1, position matches that signal")
    print("  -> OTHER_REASON: Unexplained")
    print("- MAGNITUDE_ERROR: Right direction but >2% weight difference")
    print("- MATCHED: Within tolerance")
    print("=" * 70)


if __name__ == '__main__':
    main()
