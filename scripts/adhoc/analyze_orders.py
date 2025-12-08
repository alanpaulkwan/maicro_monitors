#!/usr/bin/env python3
"""Ad hoc analysis of recent Hyperliquid orders.

Outputs:
1) Last N orders (default 2000) grouped by day with counts.
2) Day-span coverage stats: how many calendar days are present and how many are missing within the range of those orders.
3) Coverage vs Jianan targets (maicro_logs.positions_jianan_v6):
   - Deduplicate by earliest inserted_at per (trade_date, symbol) using LIMIT 1 BY.
   - Execution assumed at trade_date + 2 days (00:00 UTC).
   - Check if an order exists for that symbol on execution date and whether its side matches the desired sign.
"""
import os
import sys
from datetime import date
import pandas as pd

# Make repo modules importable when run from anywhere
# __file__ = .../scripts/adhoc/analyze_orders.py → repo root is three levels up
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(REPO_ROOT)

from modules.clickhouse_client import query_df  # noqa: E402

DEFAULT_LIMIT = int(os.getenv("ORDER_ANALYSIS_LIMIT", "2000"))

def _compress_dates(dates):
    """Compress sorted date list into ranges for compact display."""
    if not dates:
        return []
    ranges = []
    start = prev = dates[0]
    for d in dates[1:]:
        if (d - prev).days > 1:
            ranges.append((start, prev))
            start = d
        prev = d
    ranges.append((start, prev))
    return ranges

def main(limit: int = DEFAULT_LIMIT):
    sql = f"""
        SELECT coin, side, limitPx, sz, oid, timestamp, status, orderType, reduceOnly
        FROM maicro_monitors.orders
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    df = query_df(sql)
    if df.empty:
        print("No orders returned from maicro_monitors.orders")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date

    # Daily counts
    daily = df.groupby('date').size().reset_index(name='orders').sort_values('date')

    start_date = daily['date'].min()
    end_date = daily['date'].max()
    span_days = (end_date - start_date).days + 1
    present_days = daily.shape[0]

    full_dates = pd.date_range(start_date, end_date, freq='D').date
    missing_dates = [d for d in full_dates if d not in set(daily['date'])]
    missing_ranges = _compress_dates(sorted(missing_dates))

    print(f"Analyzing last {limit} orders from maicro_monitors.orders\n")
    print("Time range (in returned slice):")
    print(f"  first order: {df['timestamp'].min()} UTC")
    print(f"  last  order: {df['timestamp'].max()} UTC")
    print(f"  span days:  {span_days}")
    print()

    print("Daily order counts (chronological):")
    print(daily.to_string(index=False))
    print()

    print("Coverage summary:")
    print(f"  days with orders: {present_days}")
    print(f"  days missing:    {len(missing_dates)}")
    if missing_ranges:
        pretty = ", ".join(
            f"{a}→{b}" if a != b else f"{a}"
            for a, b in missing_ranges
        )
        print(f"  missing ranges: {pretty}")
    else:
        print("  missing ranges: none")

    print()
    print("Top 10 days by order count (within slice):")
    top10 = daily.sort_values('orders', ascending=False).head(10)
    print(top10.to_string(index=False))

    analyze_coverage(df)


def analyze_coverage(orders_df: pd.DataFrame):
    """Compare deduped Jianan targets to orders placed on execution date."""
    if orders_df.empty:
        print("\n[Coverage] No orders to compare.")
        return

    min_ord_date = orders_df['date'].min()
    max_ord_date = orders_df['date'].max()

    # Trade dates whose execution date (date + 2) lands in the order span
    trade_start = pd.to_datetime(min_ord_date) - pd.Timedelta(days=2)
    trade_end = pd.to_datetime(max_ord_date) - pd.Timedelta(days=2)

    sql_positions = """
        SELECT date,
               symbol,
               weight,
               weight_intraday,
               weight_daily,
               pred_ret,
               inserted_at
        FROM (
            SELECT
                date,
                symbol,
                weight,
                weight_intraday,
                weight_daily,
                pred_ret,
                inserted_at
            FROM maicro_logs.positions_jianan_v6
            WHERE date BETWEEN %(d0)s AND %(d1)s
              AND weight IS NOT NULL
            ORDER BY date, symbol, inserted_at ASC
            LIMIT 1 BY date, symbol
        )
    """

    pos_df = query_df(sql_positions, params={"d0": trade_start.date(), "d1": trade_end.date()})
    if pos_df.empty:
        print("\n[Coverage] No deduped targets found in the relevant date window.")
        return

    pos_df['date'] = pd.to_datetime(pos_df['date']).dt.date
    pos_df['execution_date'] = (pd.to_datetime(pos_df['date']) + pd.Timedelta(days=2)).dt.date
    pos_df['desired_side'] = pos_df['weight'].apply(lambda w: 'B' if w > 0 else ('A' if w < 0 else None))
    pos_df['symbol_norm'] = pos_df['symbol'].str.upper()

    orders_daily = (
        orders_df.groupby(['date', 'coin'])['side']
        .apply(list)
        .reset_index(name='sides')
    )

    merged = pos_df.merge(
        orders_daily,
        left_on=['execution_date', 'symbol_norm'],
        right_on=['date', 'coin'],
        how='left',
        suffixes=('_pos', '_ord')
    )

    merged = merged.rename(columns={
        'date_pos': 'trade_date',
        'execution_date': 'exec_date',
        'date_ord': 'order_date'
    })

    merged['has_order'] = merged['sides'].notna()
    merged['side_match'] = merged.apply(
        lambda r: r['desired_side'] in r['sides'] if isinstance(r['sides'], list) and r['desired_side'] else False,
        axis=1
    )

    total_targets = len(merged)
    with_orders = merged['has_order'].sum()
    correct_side = merged['side_match'].sum()
    missing = total_targets - with_orders
    wrong_side = with_orders - correct_side

    print("\n[Coverage] Orders vs Jianan targets (deduped earliest inserted_at per (date, symbol))")
    print(f"  Trade dates considered: {pos_df['date'].min()} → {pos_df['date'].max()} (exec dates {merged['exec_date'].min()} → {merged['exec_date'].max()})")
    print(f"  Targets: {total_targets}")
    print(f"  With any order on execution date: {with_orders}")
    print(f"  Correct side: {correct_side}")
    print(f"  Missing orders: {missing}")
    print(f"  Wrong side: {wrong_side}")

    missing_examples = merged.loc[~merged['has_order']].head(10)
    if not missing_examples.empty:
        print("\n  Sample missing (first 10):")
        print(missing_examples[['trade_date', 'exec_date', 'symbol', 'weight', 'desired_side']].to_string(index=False))

    wrong_examples = merged.loc[merged['has_order'] & ~merged['side_match']].head(10)
    if not wrong_examples.empty:
        print("\n  Sample wrong side (first 10):")
        print(wrong_examples[['trade_date', 'exec_date', 'symbol', 'weight', 'desired_side', 'sides']].to_string(index=False))

if __name__ == "__main__":
    limit_env = os.getenv("ORDER_ANALYSIS_LIMIT")
    try:
        limit_arg = int(sys.argv[1]) if len(sys.argv) > 1 else None
    except ValueError:
        limit_arg = None
    limit = limit_arg or DEFAULT_LIMIT
    main(limit)
