#!/usr/bin/env python3
"""
Scheduled job: build hourly positions timeline from trades for the latest day.

This wraps scripts/diagnosis_lawrence_trades/hourly_timeline_from_trades.py
and runs it for a single target date (by default: yesterday, UTC),
then upserts into ClickHouse table maicro_tmp.hourly_timeline_lawrence.
"""

import os
import sys
from datetime import datetime, timedelta, date
from typing import List, Set

import pandas as pd

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir)))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from modules.clickhouse_client import query_df  # type: ignore  # noqa: E402
from scripts.diagnosis_lawrence_trades import (  # type: ignore  # noqa: E402
    hourly_timeline_from_trades,
)


def _default_target_date_utc() -> date:
    """Return the default target date (yesterday, UTC)."""
    today_utc = datetime.utcnow().date()
    return today_utc - timedelta(days=1)


def _find_missing_dates() -> List[date]:
    """
    Find all trade dates with no corresponding rows in the hourly timeline table.

    We look at the full date range in maicro_monitors.trades and compare against
    distinct dates present in maicro_tmp.hourly_timeline_lawrence.
    """
    # Get trade date range
    df_range = query_df(
        """
        SELECT
            toDate(min(time)) AS min_date,
            toDate(max(time)) AS max_date
        FROM maicro_monitors.trades
        """
    )
    if df_range.empty or pd.isna(df_range.iloc[0]["min_date"]):
        return []

    min_date = pd.to_datetime(df_range.iloc[0]["min_date"]).date()
    max_date = pd.to_datetime(df_range.iloc[0]["max_date"]).date()

    # Don't try to build future / current partial day
    today_utc = datetime.utcnow().date()
    if max_date >= today_utc:
        max_date = today_utc - timedelta(days=1)
    if max_date < min_date:
        return []

    # Existing dates in hourly_timeline_lawrence
    df_existing = query_df(
        """
        SELECT DISTINCT toDate(ts_hour) AS d
        FROM maicro_tmp.hourly_timeline_lawrence
        """
    )
    existing: Set[date] = set()
    if not df_existing.empty:
        existing = {pd.to_datetime(d).date() for d in df_existing["d"].tolist()}

    all_dates: List[date] = []
    cur = min_date
    while cur <= max_date:
        all_dates.append(cur)
        cur += timedelta(days=1)

    missing = [d for d in all_dates if d not in existing]
    return sorted(missing)


def _build_and_write_for_date(d: date) -> None:
    """Helper to build + write hourly timeline for a single date."""
    ds = d.strftime("%Y-%m-%d")
    print(f"[build_hourly_timeline_daily] Building hourly timeline for {ds} (UTC date)")

    df = hourly_timeline_from_trades.build_hourly_timeline(
        start_date=ds,
        end_date=ds,
    )
    if df.empty:
        print(
            f"[build_hourly_timeline_daily] No data returned for {ds}; "
            "nothing to write."
        )
        return

    try:
        hourly_timeline_from_trades.write_hourly_table(
            df,
            start_date=ds,
            end_date=ds,
        )
        print(
            "[build_hourly_timeline_daily] "
            f"Wrote hourly timeline to maicro_tmp.hourly_timeline_lawrence for {ds}"
        )
    except Exception as e:  # pragma: no cover - ClickHouse failure path
        print(
            f"[build_hourly_timeline_daily] ERROR writing hourly timeline "
            f"for {ds}: {e!r}"
        )


def main() -> None:
    # 1) Repair all missing historical dates (up to yesterday)
    missing_dates = _find_missing_dates()
    if missing_dates:
        print(
            f"[build_hourly_timeline_daily] Found {len(missing_dates)} "
            "dates with missing hourly timeline; rebuilding..."
        )
        for d in missing_dates:
            _build_and_write_for_date(d)
    else:
        print(
            "[build_hourly_timeline_daily] No missing dates detected in "
            "maicro_tmp.hourly_timeline_lawrence."
        )

    # 2) Always refresh yesterday to pick up late data
    target_date = _default_target_date_utc()
    if target_date not in missing_dates:
        _build_and_write_for_date(target_date)


if __name__ == "__main__":
    main()
