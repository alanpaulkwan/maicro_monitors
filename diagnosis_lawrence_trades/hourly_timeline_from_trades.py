"""
Hourly timeline of positions from Lawrence's trades
===================================================

Goal
----
Reconstruct an hourly time series of positions from Hyperliquid trades
(`maicro_monitors.trades`), then attach:
  - Binance 1h prices (binance.bn_spot_klines)
  - Jianan model weights (maicro_logs.positions_jianan_v6) under T+2 and T+1
  - Account equity (maicro_monitors.account_snapshots)

This lets us compare:
  - actual positions vs model-expected positions,
  - under correct T+2 vs the (often used) T+1 execution rule,
on an hourly grid without doing a Python for-loop over hours.

Usage:
    python hourly_timeline_from_trades.py \\
        --start-date 2025-10-20 \\
        --end-date   2025-10-22
"""

import argparse
import io
import os
import subprocess
from datetime import date, datetime

import pandas as pd
import numpy as np


CLICKHOUSE_HOST = os.getenv(
    "CLICKHOUSE_HOST",
    os.getenv("CLICKHOUSE_LOCAL_HOST", "chenlin04.fbe.hku.hk"),
)
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "9000")


def _run_clickhouse(sql: str) -> pd.DataFrame:
    """
    Run a query via the native clickhouse-client and return a DataFrame.

    We avoid clickhouse_connect here because HTTP :8123 is not reachable in
    this environment, while the native client (port 9000) works reliably.
    """
    cmd = [
        "clickhouse-client",
        "--host",
        CLICKHOUSE_HOST,
        "--port",
        CLICKHOUSE_PORT,
        "--format",
        "CSVWithNames",
        "--query",
        sql,
    ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        # Surface ClickHouse error message to the caller
        raise RuntimeError(
            f"clickhouse-client failed with code {proc.returncode}:\n{proc.stderr}"
        )
    return pd.read_csv(io.StringIO(proc.stdout))


def _execute_clickhouse(sql: str, data: str | None = None, use_csv_with_names: bool = False) -> str:
    """
    Execute an arbitrary ClickHouse statement (DDL/DML).

    If `data` is provided, it is piped to stdin (for INSERT ... FORMAT CSVWithNames).
    """
    cmd = [
        "clickhouse-client",
        "--host",
        CLICKHOUSE_HOST,
        "--port",
        CLICKHOUSE_PORT,
        "--query",
        sql,
    ]
    if use_csv_with_names:
        cmd.extend(["--format", "CSVWithNames"])

    proc = subprocess.run(
        cmd,
        input=data,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"clickhouse-client failed (exec) with code {proc.returncode}:\n{proc.stderr}"
        )
    return proc.stdout


def get_trades_date_range() -> tuple[date, date]:
    """Return (min_date, max_date) for maicro_monitors.trades."""
    sql = """
        SELECT
            toDate(min(time)) AS min_date,
            toDate(max(time)) AS max_date
        FROM maicro_monitors.trades
    """
    res = _run_clickhouse(sql)
    if res.empty or res.iloc[0, 0] is None:
        raise RuntimeError("maicro_monitors.trades is empty; nothing to do.")
    min_date, max_date = res.iloc[0, 0], res.iloc[0, 1]
    return min_date, max_date


def build_hourly_timeline(
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Build hourly positions timeline from maicro_monitors.trades.

    For each (hour, symbol) pair we compute:
      - pos_units   : net position size (contracts/coins)
      - bn_px       : Binance spot 1h close (mapped sym → symUSDT)
      - equity_usd  : hourly-averaged account value
      - weight_t2   : model target weight with T+2 execution (signal_date + 2)
      - weight_t1   : model target weight with T+1 execution (signal_date + 1)

    Implementation is fully in ClickHouse:
      1) Generate hour grid via system.numbers
      2) CROSS JOIN with symbol universe
      3) Aggregate trades to hourly deltas (side B = +sz, side A = -sz)
      4) Cumulative sum over hours per symbol, plus initial position before start
      5) LEFT JOIN Binance prices, equity, and model weights
    """

    # 1) Hourly positions + Binance price + equity (pure SQL)
    sql_timeline = f"""
    WITH
        toDateTime('{start_date} 00:00:00') AS start_ts,
        toDateTime('{end_date} 23:00:00')   AS end_ts,

        hours AS (
            SELECT
                (start_ts + number * 3600) AS ts_hour
            FROM system.numbers
            LIMIT dateDiff('hour', start_ts, end_ts) + 1
        ),

        symbols AS (
            SELECT DISTINCT upper(coin) AS sym
            FROM maicro_monitors.trades
            WHERE time <= end_ts
        ),

        grid AS (
            SELECT
                h.ts_hour,
                s.sym
            FROM hours AS h
            CROSS JOIN symbols AS s
        ),

        initial_pos AS (
            SELECT
                upper(coin) AS sym,
                sum(if(side = 'B', sz, -sz)) AS pos0
            FROM maicro_monitors.trades
            WHERE time < start_ts
            GROUP BY sym
        ),

        trades_hourly AS (
            SELECT
                toStartOfHour(time) AS ts_hour,
                upper(coin)         AS sym,
                sum(if(side = 'B', sz, -sz)) AS delta_pos
            FROM maicro_monitors.trades
            WHERE time >= start_ts
              AND time <= end_ts
            GROUP BY ts_hour, sym
        ),

        grid_with_trades AS (
            SELECT
                g.ts_hour,
                g.sym,
                coalesce(t.delta_pos, 0) AS delta_pos
            FROM grid AS g
            LEFT JOIN trades_hourly AS t
                ON g.ts_hour = t.ts_hour
               AND g.sym     = t.sym
        ),

        positions_hourly AS (
            SELECT
                g.ts_hour,
                g.sym,
                coalesce(ip.pos0, 0)
                + sum(g.delta_pos) OVER (
                    PARTITION BY g.sym
                    ORDER BY g.ts_hour
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                  ) AS pos_units
            FROM grid_with_trades AS g
            LEFT JOIN initial_pos AS ip
                ON g.sym = ip.sym
        ),

        bn_hourly AS (
            SELECT
                toStartOfHour(timestamp) AS ts_hour,
                symbol,
                anyLast(close)          AS close
            FROM binance.bn_spot_klines
            WHERE interval  = '1h'
              AND timestamp >= start_ts
              AND timestamp <= end_ts
            GROUP BY ts_hour, symbol
        ),

        positions_with_px AS (
            SELECT
                p.ts_hour,
                p.sym,
                p.pos_units,
                b.close AS bn_px
            FROM positions_hourly AS p
            LEFT JOIN bn_hourly AS b
                ON b.ts_hour = p.ts_hour
               AND b.symbol = concat(p.sym, 'USDT')
        ),

        equity_hourly AS (
            SELECT
                toStartOfHour(timestamp) AS ts_hour,
                avg(accountValue)       AS equity_usd
            FROM maicro_monitors.account_snapshots
            WHERE timestamp >= start_ts
              AND timestamp <= end_ts
            GROUP BY ts_hour
        ),

        with_equity AS (
            SELECT
                p.ts_hour,
                p.sym,
                p.pos_units,
                p.bn_px,
                e.equity_usd
            FROM positions_with_px AS p
            LEFT JOIN equity_hourly AS e
                ON e.ts_hour = p.ts_hour
        )

    SELECT
        ts_hour,
        sym,
        pos_units,
        bn_px,
        equity_usd
    FROM with_equity
    ORDER BY ts_hour, sym
    """

    df = _run_clickhouse(sql_timeline)

    # 2) Load model target weights and align in pandas (T+2 and T+1)
    sql_targets = f"""
        SELECT
            toDate(date)  AS signal_date,
            upper(symbol) AS sym,
            argMax(weight, inserted_at) AS target_weight
        FROM maicro_logs.positions_jianan_v6
        WHERE date BETWEEN toDate('{start_date}') - 3 AND toDate('{end_date}')
        GROUP BY signal_date, sym
    """
    targets = _run_clickhouse(sql_targets)
    if not targets.empty:
        # Build T+2 and T+1 holdings dates
        targets["signal_date"] = pd.to_datetime(targets["signal_date"])
        targets["holdings_date_t2"] = targets["signal_date"] + pd.to_timedelta(
            2, unit="D"
        )
        targets["holdings_date_t1"] = targets["signal_date"] + pd.to_timedelta(
            1, unit="D"
        )

        t2 = targets[["sym", "holdings_date_t2", "target_weight"]].rename(
            columns={"holdings_date_t2": "holdings_date", "target_weight": "weight_t2"}
        )
        t1 = targets[["sym", "holdings_date_t1", "target_weight"]].rename(
            columns={"holdings_date_t1": "holdings_date", "target_weight": "weight_t1"}
        )

        # Merge onto hourly timeline by holdings_date = date(ts_hour)
        df["holdings_date"] = pd.to_datetime(df["ts_hour"]).dt.normalize()
        df = df.merge(
            t2,
            how="left",
            left_on=["sym", "holdings_date"],
            right_on=["sym", "holdings_date"],
        )
        df = df.merge(
            t1,
            how="left",
            left_on=["sym", "holdings_date"],
            right_on=["sym", "holdings_date"],
        )
        df = df.drop(columns=["holdings_date"])
    else:
        df["weight_t2"] = pd.NA
        df["weight_t1"] = pd.NA

    return df


def write_hourly_table(df: pd.DataFrame, start_date: str, end_date: str) -> None:
    """
    Persist hourly timeline into ClickHouse: maicro_tmp.hourly_timeline_lawrence.

    We upsert by date range: delete any existing rows for [start_ts, end_ts],
    then INSERT the new rows.
    """
    # Ensure columns exist
    expected_cols = [
        "ts_hour",
        "sym",
        "pos_units",
        "bn_px",
        "equity_usd",
        "weight_t2",
        "weight_t1",
    ]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in hourly DataFrame: {missing}")

    # Create table if needed
    create_sql = """
    CREATE TABLE IF NOT EXISTS maicro_tmp.hourly_timeline_lawrence
    (
        ts_hour    DateTime,
        sym        String,
        pos_units  Float64,
        bn_px      Float64,
        equity_usd Float64,
        weight_t2  Nullable(Float64),
        weight_t1  Nullable(Float64)
    )
    ENGINE = MergeTree
    ORDER BY (ts_hour, sym)
    """
    _execute_clickhouse(create_sql)

    start_ts = f"{start_date} 00:00:00"
    end_ts = f"{end_date} 23:00:00"

    # Delete overlapping rows for this range to avoid duplicates on rerun
    delete_sql = f"""
        ALTER TABLE maicro_tmp.hourly_timeline_lawrence
        DELETE WHERE ts_hour >= toDateTime('{start_ts}')
               AND ts_hour <= toDateTime('{end_ts}')
    """
    _execute_clickhouse(delete_sql)

    # Prepare CSV payload
    df_to_write = df[expected_cols].copy()
    # Ensure ts_hour is in a ClickHouse-friendly string format
    df_to_write["ts_hour"] = pd.to_datetime(df_to_write["ts_hour"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    csv_data = df_to_write.to_csv(index=False, na_rep="\\N")

    insert_sql = """
        INSERT INTO maicro_tmp.hourly_timeline_lawrence
        (ts_hour, sym, pos_units, bn_px, equity_usd, weight_t2, weight_t1)
        FORMAT CSVWithNames
    """
    _execute_clickhouse(insert_sql, data=csv_data, use_csv_with_names=True)


def main():
    parser = argparse.ArgumentParser(
        description="Reconstruct hourly positions timeline from trades."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD). Default: first trade date.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD). Default: last trade date.",
    )
    parser.add_argument(
        "--limit-symbols",
        type=str,
        default="",
        help="Comma-separated subset of symbols to print (e.g. 'BTC,ETH,ARB').",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=200,
        help="Max rows to print from the hourly timeline.",
    )
    args = parser.parse_args()

    min_date, max_date = get_trades_date_range()

    start_date = args.start_date or str(min_date)
    end_date = args.end_date or str(max_date)

    print(f"Using trade date range: {start_date} to {end_date}")

    df = build_hourly_timeline(start_date=start_date, end_date=end_date)

    # Persist full-universe hourly table before any symbol filtering
    try:
        write_hourly_table(df, start_date=start_date, end_date=end_date)
        print(
            "Wrote hourly timeline to ClickHouse table "
            "maicro_tmp.hourly_timeline_lawrence"
        )
    except Exception as e:
        print(f"WARNING: failed to write hourly table to ClickHouse: {e}")

    if df.empty:
        print("No data returned for the requested range.")
        return

    # Optional symbol filter for readability
    symbols_filter = [
        s.strip().upper() for s in args.limit_symbols.split(",") if s.strip()
    ]
    if symbols_filter:
        df = df[df["sym"].isin(symbols_filter)]

    # Basic sanity checks
    print("\nSanity checks:")
    print(f"- Rows returned: {len(df):,}")
    print(f"- Unique symbols: {df['sym'].nunique()}")
    print(f"- Time span: {df['ts_hour'].min()}  →  {df['ts_hour'].max()}")

    non_null_px = df["bn_px"].notnull().mean()
    non_null_equity = df["equity_usd"].notnull().mean()
    print(f"- Fraction with Binance price: {non_null_px:.2%}")
    print(f"- Fraction with equity_usd:   {non_null_equity:.2%}")

    # Tracking error diagnostics (weights-based)
    print("\nTracking error vs model weights (by day):")
    df_te = df.copy()
    df_te["date"] = pd.to_datetime(df_te["ts_hour"]).dt.date

    mask_valid = (
        df_te["equity_usd"].notnull()
        & (df_te["equity_usd"] != 0)
        & df_te["bn_px"].notnull()
    )
    df_te = df_te[mask_valid].copy()
    if not df_te.empty:
        df_te["actual_weight"] = (
            df_te["pos_units"] * df_te["bn_px"] / df_te["equity_usd"]
        )

        # Precompute basic errors
        df_te["err_t2"] = df_te["actual_weight"] - df_te["weight_t2"]
        df_te["err_t1"] = df_te["actual_weight"] - df_te["weight_t1"]

        def summarize(offset: str) -> pd.DataFrame | None:
            col = f"weight_{offset}"
            if col not in df_te.columns:
                return None
            m = df_te[df_te[col].notnull() & df_te["actual_weight"].notnull()].copy()
            if m.empty:
                return None
            m["err"] = m["actual_weight"] - m[col]
            grp = m.groupby("date")["err"]
            out = pd.DataFrame(
                {
                    "rmse": grp.apply(lambda x: float((x**2).mean() ** 0.5)),
                    "mae": grp.apply(lambda x: float(x.abs().mean())),
                    "mean_err": grp.mean().astype(float),
                }
            )
            return out

        te_t2 = summarize("t2")
        te_t1 = summarize("t1")

        if te_t2 is not None:
            print("\n▼ Daily tracking error vs T+2 weights")
            print(te_t2.to_string())
        else:
            print("\n(no valid rows for T+2 tracking error)")

        if te_t1 is not None:
            print("\n▼ Daily tracking error vs T+1 weights")
            print(te_t1.to_string())
        else:
            print("\n(no valid rows for T+1 tracking error)")

        # -------------------------------
        # Taxonomy of tracking error
        # -------------------------------
        print("\nTaxonomy of tracking error vs T+2 (squared-error share):")
        te = df_te[
            df_te["weight_t2"].notnull()
            & df_te["actual_weight"].notnull()
        ].copy()
        if te.empty:
            print("(no rows with both actual_weight and weight_t2)")
        else:
            W_EPS = 1e-3   # 0.1% threshold for "non-zero" weight
            MAG_EPS = 0.02  # 2% threshold for magnitude errors

            is_target_big = te["weight_t2"].abs() >= W_EPS
            is_actual_big = te["actual_weight"].abs() >= W_EPS

            # Default category
            category = np.array(["OTHER"] * len(te), dtype=object)

            # Both effectively zero
            mask_zero = (~is_target_big) & (~is_actual_big)
            category[mask_zero] = "MATCHED_ZERO"

            # Missing: model wants size, we have ~0
            mask_missing = is_target_big & (~is_actual_big)
            category[mask_missing] = "MISSING_POSITION"

            # Extra: we have size, model wants ~0
            mask_extra = (~is_target_big) & is_actual_big
            category[mask_extra] = "EXTRA_POSITION"

            # Wrong direction
            same_sign = np.sign(te["weight_t2"]) == np.sign(te["actual_weight"])
            mask_wrong_dir = (
                is_target_big & is_actual_big & (~same_sign)
            )
            category[mask_wrong_dir] = "WRONG_DIRECTION"

            # Magnitude error: same sign, big weights, but diff > MAG_EPS
            mask_mag_err = (
                is_target_big
                & is_actual_big
                & same_sign
                & (te["err_t2"].abs() > MAG_EPS)
            )
            category[mask_mag_err] = "MAGNITUDE_ERROR"

            # Matched (non-zero) where within tolerance
            mask_matched_nonzero = (
                (is_target_big | is_actual_big)
                & (te["err_t2"].abs() <= MAG_EPS)
                & (~mask_missing)
                & (~mask_extra)
                & (~mask_wrong_dir)
                & (~mask_mag_err)
            )
            category[mask_matched_nonzero] = "MATCHED"

            te["category"] = category
            te["se_t2"] = te["err_t2"] ** 2

            total_se = float(te["se_t2"].sum())
            if total_se <= 0:
                print("(total squared error is zero)")
            else:
                by_cat = (
                    te.groupby("category")["se_t2"]
                    .sum()
                    .sort_values(ascending=False)
                )
                for cat, se in by_cat.items():
                    share = se / total_se
                    print(f"- {cat:18s}: {share:6.1%}")

                # Flip attribution: where T+1 explains better than T+2
                has_t1 = te["weight_t1"].notnull()
                closer_t1 = has_t1 & (
                    te["err_t1"].abs() + 1e-9 < te["err_t2"].abs()
                )
                te["flip_explained"] = closer_t1

                by_cat_flip = (
                    te.groupby(["category", "flip_explained"])["se_t2"]
                    .sum()
                    .reset_index()
                )

                print("\nWithin each category, share of TE due to flips (T+1 closer than T+2):")
                for cat in by_cat.index:
                    rows = by_cat_flip[by_cat_flip["category"] == cat]
                    se_cat = float(by_cat.loc[cat])
                    if se_cat <= 0:
                        continue
                    se_flip = float(
                        rows.loc[
                            rows["flip_explained"] == True, "se_t2"
                        ].sum()
                    )
                    share_flip = se_flip / se_cat if se_cat > 0 else 0.0
                    print(f"- {cat:18s}: {share_flip:6.1%} of that category's TE")
    else:
        print("Not enough data to compute tracking error (missing prices/equity).")

    # Show a small preview
    print("\nSample of hourly timeline:")
    print(
        df.sort_values(["ts_hour", "sym"])
        .head(args.rows)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
