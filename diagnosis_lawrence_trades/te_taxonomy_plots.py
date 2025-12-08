"""
Generate graphical evidence for tracking error taxonomy.

This script:
  1. Loads hourly positions/weights from maicro_tmp.hourly_timeline_lawrence
  2. Reconstructs:
       - actual_weight   = pos_units * bn_px / equity_usd
       - errors vs T+2 / T+1 (weights)
       - taxonomy categories:
           * MATCHED_ZERO
           * MISSING_POSITION
           * EXTRA_POSITION
           * WRONG_DIRECTION
           * MAGNITUDE_ERROR
           * MATCHED
       - flip_explained flag where T+1 is closer than T+2
  3. Produces plots:
       - daily RMSE vs T+2 / T+1 weights
       - overall TE share by category
       -,for each category, share of TE due to flips

Usage (from repo root):
    python3 diagnosis_lawrence_trades/te_taxonomy_plots.py \\
        --start-date 2025-10-16 \\
        --end-date   2025-12-07 \\
        --output-dir diagnosis_lawrence_trades
"""

import argparse
import os
from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # non-interactive backend for CLI
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import subprocess
import io


CLICKHOUSE_HOST = os.getenv(
    "CLICKHOUSE_HOST",
    os.getenv("CLICKHOUSE_LOCAL_HOST", "chenlin04.fbe.hku.hk"),
)
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "9000")


def _run_clickhouse(sql: str) -> pd.DataFrame:
    """
    Run a ClickHouse query and return DataFrame (CSVWithNames).
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
        raise RuntimeError(
            f"clickhouse-client failed (query) with code {proc.returncode}:\n{proc.stderr}"
        )
    return pd.read_csv(io.StringIO(proc.stdout))


def load_hourly(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Load hourly data from maicro_tmp.hourly_timeline_lawrence for [start_date, end_date].
    """
    sql = f"""
        SELECT
            ts_hour,
            sym,
            pos_units,
            bn_px,
            equity_usd,
            weight_t2,
            weight_t1
        FROM maicro_tmp.hourly_timeline_lawrence
        WHERE ts_hour >= toDateTime('{start_date} 00:00:00')
          AND ts_hour <= toDateTime('{end_date} 23:00:00')
        ORDER BY ts_hour, sym
    """
    df = _run_clickhouse(sql)
    # Enforce numeric dtypes for downstream arithmetic
    for col in ["pos_units", "bn_px", "equity_usd", "weight_t2", "weight_t1"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if df.empty:
        raise RuntimeError("hourly timeline table returned no rows for this range")
    return df


def build_te_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich hourly DataFrame with:
      - date
      - actual_weight, err_t2, err_t1
      - category (taxonomy)
      - flip_explained (T+1 closer than T+2)
    """
    df = df.copy()
    df["ts_hour"] = pd.to_datetime(df["ts_hour"])
    df["date"] = df["ts_hour"].dt.date

    # Guard against zero equity
    mask_valid = (
        df["equity_usd"].notnull()
        & (df["equity_usd"] != 0)
        & df["bn_px"].notnull()
    )
    df = df[mask_valid].copy()

    df["actual_weight"] = df["pos_units"] * df["bn_px"] / df["equity_usd"]
    df["err_t2"] = df["actual_weight"] - df["weight_t2"]
    df["err_t1"] = df["actual_weight"] - df["weight_t1"]

    # Taxonomy vs T+2
    W_EPS = 1e-3
    MAG_EPS = 0.02

    is_target_big = df["weight_t2"].abs() >= W_EPS
    is_actual_big = df["actual_weight"].abs() >= W_EPS

    category = np.array(["OTHER"] * len(df), dtype=object)

    mask_zero = (~is_target_big) & (~is_actual_big)
    category[mask_zero] = "MATCHED_ZERO"

    mask_missing = is_target_big & (~is_actual_big)
    category[mask_missing] = "MISSING_POSITION"

    mask_extra = (~is_target_big) & is_actual_big
    category[mask_extra] = "EXTRA_POSITION"

    same_sign = np.sign(df["weight_t2"]) == np.sign(df["actual_weight"])
    mask_wrong_dir = is_target_big & is_actual_big & (~same_sign)
    category[mask_wrong_dir] = "WRONG_DIRECTION"

    mask_mag_err = (
        is_target_big
        & is_actual_big
        & same_sign
        & (df["err_t2"].abs() > MAG_EPS)
    )
    category[mask_mag_err] = "MAGNITUDE_ERROR"

    mask_matched_nonzero = (
        (is_target_big | is_actual_big)
        & (df["err_t2"].abs() <= MAG_EPS)
        & (~mask_missing)
        & (~mask_extra)
        & (~mask_wrong_dir)
        & (~mask_mag_err)
    )
    category[mask_matched_nonzero] = "MATCHED"

    df["category"] = category
    df["se_t2"] = df["err_t2"] ** 2

    # Flip attribution
    has_t1 = df["weight_t1"].notnull()
    df["flip_explained"] = has_t1 & (
        df["err_t1"].abs() + 1e-9 < df["err_t2"].abs()
    )

    return df


def plot_daily_rmse(df: pd.DataFrame, out_path: str) -> None:
    """
    Plot daily RMSE vs T+2 and T+1 weights.
    """
    m = df.dropna(subset=["actual_weight", "weight_t2"])

    # T+2
    g2 = m.groupby("date")["err_t2"]
    rmse_t2 = g2.apply(lambda x: float((x**2).mean() ** 0.5))

    # T+1
    m1 = m.dropna(subset=["weight_t1"])
    g1 = m1.groupby("date")["err_t1"]
    rmse_t1 = g1.apply(lambda x: float((x**2).mean() ** 0.5))

    dates = sorted(set(rmse_t2.index) | set(rmse_t1.index))
    d_idx = pd.to_datetime(pd.Series(dates))

    plt.figure(figsize=(10, 4))
    plt.plot(d_idx, rmse_t2.reindex(dates), label="RMSE vs T+2", marker="o")
    plt.plot(d_idx, rmse_t1.reindex(dates), label="RMSE vs T+1", marker="o")
    plt.ylabel("Daily RMSE (weight)")
    plt.xlabel("Date")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_te_share_by_category(df: pd.DataFrame, out_path: str) -> None:
    """
    Bar chart: overall TE share by category (squared error vs T+2).
    """
    by_cat = (
        df.groupby("category")["se_t2"]
        .sum()
        .sort_values(ascending=False)
    )
    total_se = float(by_cat.sum())
    shares = by_cat / total_se if total_se > 0 else by_cat * 0

    plt.figure(figsize=(8, 4))
    plt.bar(shares.index, shares.values)
    plt.ylabel("Share of total squared TE")
    plt.xticks(rotation=45, ha="right")
    plt.ylim(0, 1)
    for i, (cat, val) in enumerate(shares.items()):
        plt.text(i, val + 0.01, f"{val:0.1%}", ha="center", va="bottom", fontsize=8)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_flip_share_by_category(df: pd.DataFrame, out_path: str) -> None:
    """
    For each category, plot share of that category's TE where T+1 is closer than T+2.
    """
    by_cat = (
        df.groupby("category")["se_t2"]
        .sum()
        .sort_values(ascending=False)
    )
    total_by_cat = by_cat.to_dict()

    flip = df[df["flip_explained"]]
    flip_by_cat = flip.groupby("category")["se_t2"].sum().to_dict()

    cats = list(by_cat.index)
    shares = []
    for cat in cats:
        se_cat = total_by_cat.get(cat, 0.0)
        se_flip = flip_by_cat.get(cat, 0.0)
        share = se_flip / se_cat if se_cat > 0 else 0.0
        shares.append(share)

    plt.figure(figsize=(8, 4))
    plt.bar(cats, shares)
    plt.ylabel("Within-category TE share due to flips")
    plt.xticks(rotation=45, ha="right")
    plt.ylim(0, 1)
    for i, (cat, val) in enumerate(zip(cats, shares)):
        plt.text(i, val + 0.01, f"{val:0.1%}", ha="center", va="bottom", fontsize=8)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate plots backing tracking error taxonomy."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2025-10-16",
        help="Start date (YYYY-MM-DD) for plots.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2025-12-07",
        help="End date (YYYY-MM-DD) for plots.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="diagnosis_lawrence_trades",
        help="Directory to write PNG figures.",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    df_hourly = load_hourly(args.start_date, args.end_date)
    df_te = build_te_frame(df_hourly)

    print(
        f"Loaded {len(df_te):,} hourly rows for "
        f"{df_te['date'].min()} → {df_te['date'].max()}"
    )

    # 1) Daily RMSE vs T+2 / T+1
    out_rmse = os.path.join(args.output_dir, "te_daily_rmse_t2_vs_t1.png")
    plot_daily_rmse(df_te, out_rmse)
    print(f"Wrote daily RMSE plot: {out_rmse}")

    # 2) Overall TE share by category
    out_cat = os.path.join(args.output_dir, "te_share_by_category.png")
    plot_te_share_by_category(df_te, out_cat)
    print(f"Wrote TE share-by-category plot: {out_cat}")

    # 3) Flip share by category
    out_flip = os.path.join(args.output_dir, "te_flip_share_by_category.png")
    plot_flip_share_by_category(df_te, out_flip)
    print(f"Wrote flip share-by-category plot: {out_flip}")


if __name__ == "__main__":
    main()
