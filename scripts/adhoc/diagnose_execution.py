#!/usr/bin/env python3
"""Incremental execution diagnosis per day and offset.

What this script does (plain):
1) Pull targets (earliest row per date/symbol from positions_jianan_v6 with finite, non-zero weight & pred_ret).
2) Assume execution date = trade_date + --offset (0/1/2).
3) Pull orders on that exec_date (maicro_monitors.orders) and positions snapshots (live_positions, kind='current').
4) For each target symbol on each trade_date, classify:
   - missing_order_and_position (nothing seen)
   - missing_order (position exists, no order)
   - order_wrong_side (order present, opposite target sign; beware this may be a legit reduction)
   - missing_position (order present, no position snapshot)
   - position_wrong_side (position sign opposite target; could be mid-rebalance)
   - correct (order present & position sign matches target sign)
   - extra_asset (position exists with no target) reported separately
5) Heuristic flags for min_usd/min_units using hl_meta (proxy notional = abs(weight)*capital_proxy).

This is descriptive, not prescriptive: reductions/closing trades can appear as “wrong side” when comparing to raw target sign.

Objective for review:
- Provide a quick, stage-wise accounting of where symbols drop between target weights, orders, and resulting positions for a given execution offset.
- Quantify whether min_usd/min_units could explain missing orders (via a simple proxy) and surface sample rows for each failure bucket.
- Keep the script simple/print-driven so it can be run ad hoc to guide debugging and future instrumentation.
"""
import os
import sys
import argparse
import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(REPO_ROOT)
from modules.clickhouse_client import query_df  # type: ignore


def load_targets(start, end):
    sql = """
        SELECT date, symbol, weight
        FROM (
            SELECT date, symbol, weight, inserted_at
            FROM maicro_logs.positions_jianan_v6
            WHERE date BETWEEN %(d0)s AND %(d1)s
              AND weight IS NOT NULL AND isFinite(weight) AND weight != 0
              AND pred_ret IS NOT NULL AND isFinite(pred_ret)
            ORDER BY date, symbol, inserted_at
            LIMIT 1 BY date, symbol
        )
    """
    df = query_df(sql, params={"d0": start, "d1": end})
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol_norm"] = df["symbol"].str.upper()
    df["target_sign"] = df["weight"].apply(lambda w: 1 if w > 0 else -1)
    return df


def load_orders(start, end):
    sql = """
        SELECT toDate(timestamp) AS date, coin AS symbol, side
        FROM maicro_monitors.orders
        WHERE toDate(timestamp) BETWEEN %(d0)s AND %(d1)s
    """
    df = query_df(sql, params={"d0": start, "d1": end})
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol_norm"] = df["symbol"].str.upper()
    return df.groupby(["date", "symbol_norm"]).side.apply(list).reset_index(name="sides")


def load_positions():
    sql = """
        SELECT * FROM (
          SELECT *, row_number() OVER(PARTITION BY target_date, symbol, kind ORDER BY ts DESC) rn
          FROM maicro_logs.live_positions
          WHERE kind='current'
        ) WHERE rn=1
    """
    df = query_df(sql)
    if df.empty:
        return df
    df["target_date"] = pd.to_datetime(df["target_date"]).dt.date
    df["symbol_norm"] = df["symbol"].str.upper()
    df["pos_sign"] = df["qty"].apply(lambda q: 1 if q > 1e-12 else (-1 if q < -1e-12 else 0))
    return df


def load_meta(symbols):
    if not symbols:
        return pd.DataFrame()
    meta = query_df(
        """
        SELECT symbol, min_usd, min_units, size_step, tick_size
        FROM maicro_logs.hl_meta
        WHERE symbol IN %(syms)s
        """,
        params={"syms": list(symbols)},
    )
    meta["symbol_norm"] = meta["symbol"].str.upper()
    return meta.set_index("symbol_norm")


def classify(offset_days: int):
    # order window
    orng = query_df("SELECT min(toDate(timestamp)) AS mn, max(toDate(timestamp)) AS mx FROM maicro_monitors.orders")
    if orng.empty or pd.isna(orng.loc[0, "mn"]):
        print("No orders found")
        return
    o_min = pd.to_datetime(orng.loc[0, "mn"]).date()
    o_max = pd.to_datetime(orng.loc[0, "mx"]).date()
    t_start = o_min - pd.Timedelta(days=offset_days)
    t_end = o_max - pd.Timedelta(days=offset_days)

    targets = load_targets(t_start, t_end)
    orders = load_orders(o_min, o_max)
    positions = load_positions()

    if targets.empty:
        print("No targets in window")
        return

    # exec date
    targets = targets.copy()
    targets["exec_date"] = (pd.to_datetime(targets["date"]) + pd.Timedelta(days=offset_days)).dt.date

    # join orders
    merged = targets.merge(
        orders,
        left_on=["exec_date", "symbol_norm"],
        right_on=["date", "symbol_norm"],
        how="left",
        suffixes=("_tgt", "_ord"),
    )
    merged["has_order"] = merged["sides"].notna()
    merged["order_side_ok"] = merged.apply(
        lambda r: r["target_sign"] == 1 and isinstance(r["sides"], list) and any(s.upper().startswith("B") for s in r["sides"])
        or r["target_sign"] == -1 and isinstance(r["sides"], list) and any(s.upper().startswith("A") for s in r["sides"]),
        axis=1,
    )

    # join positions
    merged = merged.merge(
        positions,
        left_on=["date_tgt", "symbol_norm"],
        right_on=["target_date", "symbol_norm"],
        how="left",
        suffixes=("", "_pos"),
    )
    merged["has_pos"] = merged["pos_sign"].notna()
    merged["pos_side_ok"] = merged.apply(
        lambda r: (r["target_sign"] == 1 and r.get("pos_sign", 0) == 1)
        or (r["target_sign"] == -1 and r.get("pos_sign", 0) == -1),
        axis=1,
    )

    def bucket(row):
        if not row["has_order"] and not row["has_pos"]:
            return "missing_order_and_position"
        if not row["has_order"]:
            return "missing_order"
        if not row["order_side_ok"]:
            return "order_wrong_side"
        if not row["has_pos"]:
            return "missing_position"
        if not row["pos_side_ok"]:
            return "position_wrong_side"
        return "correct"

    merged["status"] = merged.apply(bucket, axis=1)

    # meta-derived “could be below min” flag (rough heuristic at target weight using equity proxy)
    # We don't have equity per symbol here; use weight*1000 USD as a proxy to see which symbols could fall below min_usd after rounding.
    meta = load_meta(set(merged["symbol_norm"]))
    def maybe_min_violation(row):
        if row["symbol_norm"] not in meta.index:
            return False
        m = meta.loc[row["symbol_norm"]]
        # normalize to scalar
        if isinstance(m, pd.DataFrame):
            m = m.iloc[0]
        min_usd_val = m.get("min_usd", 10)
        try:
            min_usd = float(min_usd_val)
        except Exception:
            min_usd = 10.0
        if pd.isna(min_usd):
            min_usd = 10.0
        # crude proxy: assume $1000 equity, see if weight * 1000 < min_usd
        proxy_ntl = abs(row["weight"] * 1000)
        return proxy_ntl < min_usd
    merged["maybe_min_violation"] = merged.apply(maybe_min_violation, axis=1)

    per_day = merged.groupby("date_tgt")["status"].value_counts().unstack(fill_value=0).reset_index().rename(columns={"date_tgt": "date"})

    print(f"Offset +{offset_days}d | targets {len(targets)} | dates {targets['date'].min()} → {targets['date'].max()}")
    print(per_day.head())
    print("\nOverall counts:")
    print(merged["status"].value_counts())

    print("\nStatus cross-tab vs maybe_min_violation:")
    print(pd.crosstab(merged['status'], merged['maybe_min_violation']))

    print("\nSample missing_order (first 10):")
    print(merged.loc[merged["status"]=="missing_order", ["date_tgt", "symbol_norm", "weight", "maybe_min_violation"]].head(10))

    print("\nSample order_wrong_side (first 10):")
    print(merged.loc[merged["status"]=="order_wrong_side", ["date_tgt", "symbol_norm", "sides", "target_sign", "maybe_min_violation"]].head(10))

    print("\nSample missing_order_and_position (first 10):")
    print(merged.loc[merged["status"]=="missing_order_and_position", ["date_tgt", "symbol_norm", "weight", "maybe_min_violation"]].head(10))

    # extras: positions with no target
    extras = positions[~positions["symbol_norm"].isin(targets["symbol_norm"])]
    if not extras.empty:
        print("\nExtra assets (positions without targets), first 10:")
        print(extras.head(10))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offset", type=int, default=1, help="Execution offset in days (exec = trade_date + offset)")
    args = ap.parse_args()
    classify(args.offset)


if __name__ == "__main__":
    main()
