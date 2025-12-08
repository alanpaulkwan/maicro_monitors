#!/usr/bin/env python3
"""Streamlit dashboard for Hyperliquid monitoring.
Covers: recent prices, recent trades, data staleness, tracking error, historical trades, historical orders.
"""
import datetime as dt
from typing import Optional

import pandas as pd
import streamlit as st

from config.settings import HYPERLIQUID_ADDRESS, TABLE_CANDIDATES
from modules.clickhouse_client import first_existing, query_df, table_exists

st.set_page_config(page_title="Maicro Monitors", page_icon="📊", layout="wide")
st.title("📊 Maicro Monitors Dashboard")
st.caption("Hyperliquid trading accountability: prices, trades, staleness, tracking error, orders")


@st.cache_data(ttl=60)
def _get_ts_column(full_table: str) -> Optional[str]:
    db, table = full_table.split(".", 1)
    sql = (
        "SELECT name FROM system.columns "
        "WHERE database = %(db)s AND table = %(table)s"
    )
    df = query_df(sql, {"db": db, "table": table})
    preferred = [
        "trade_time",
        "order_time",
        "timestamp",
        "ts",
        "time",
        "snapshot_time",
        "date",
    ]
    cols = [c.lower() for c in df["name"].tolist()]
    for p in preferred:
        if p.lower() in cols:
            return df.loc[cols.index(p.lower()), "name"]
    return df["name"].iloc[0] if not df.empty else None


def _pick_table(kind: str) -> Optional[str]:
    candidates = TABLE_CANDIDATES.get(kind, [])
    return first_existing(candidates)


def _load_table(kind: str, limit: int = 200) -> Optional[pd.DataFrame]:
    tbl = _pick_table(kind)
    if not tbl:
        return None
    ts_col = _get_ts_column(tbl)
    order_clause = f"ORDER BY {ts_col} DESC" if ts_col else ""
    sql = f"SELECT * FROM {tbl} {order_clause} LIMIT {limit}"
    return query_df(sql)


def render_prices():
    st.subheader("Recent Prices")
    df = _load_table("prices", limit=500)
    if df is None or df.empty:
        st.warning("No price/ohlcv table found (checked candidates in config).")
        return
    ts_col = _get_ts_column(_pick_table("prices"))
    if ts_col and ts_col in df.columns:
        df_sorted = df.sort_values(ts_col)
    else:
        df_sorted = df
    st.dataframe(df.head(50))
    numeric_cols = [c for c in df_sorted.columns if c.lower() in {"close", "px", "price"}]
    if ts_col and numeric_cols:
        chart_df = df_sorted[[ts_col] + numeric_cols].set_index(ts_col)
        st.line_chart(chart_df.tail(200))


def render_recent_trades():
    st.subheader("Recent Trades")
    df = _load_table("trades", limit=200)
    if df is None or df.empty:
        st.warning("No trades table found (checked candidates in config).")
        return
    st.dataframe(df)


def render_staleness():
    st.subheader("Data Staleness (last timestamp per source)")
    rows = []
    for kind in ["prices", "trades", "orders", "tracking_error"]:
        tbl = _pick_table(kind)
        if not tbl:
            rows.append({"source": kind, "table": None, "latest_ts": None})
            continue
        ts_col = _get_ts_column(tbl)
        if not ts_col:
            rows.append({"source": kind, "table": tbl, "latest_ts": None})
            continue
        sql = f"SELECT max({ts_col}) AS latest FROM {tbl}"
        latest_df = query_df(sql)
        latest = latest_df["latest"].iloc[0] if not latest_df.empty else None
        rows.append({"source": kind, "table": tbl, "latest_ts": latest})
    st.dataframe(pd.DataFrame(rows))


def render_tracking_error():
    st.subheader("Tracking Error")
    tbl = _pick_table("tracking_error")
    if not tbl:
        st.warning("No tracking_error table found; expected maicro_monitors.tracking_error_daily.")
        return
    df = query_df(
        f"""
        SELECT * FROM {tbl}
        ORDER BY date DESC
        LIMIT 120
        """
    )
    if df.empty:
        st.info("Tracking error table is empty.")
        return
    df = df.sort_values("date")
    metrics_cols = [c for c in df.columns if c.startswith("te_") or c == "tracking_error"]
    st.dataframe(df.tail(30))
    if metrics_cols:
        chart_df = df.set_index("date")[metrics_cols]
        st.line_chart(chart_df)


def render_historical_trades():
    st.subheader("Historical Trades (up to 1000)")
    df = _load_table("trades", limit=1000)
    if df is None or df.empty:
        st.warning("No trades table found.")
        return
    st.dataframe(df)


def render_historical_orders():
    st.subheader("Historical Orders (up to 1000)")
    tbl = _pick_table("orders")
    if not tbl:
        st.warning("No orders table found (checked candidates in config).")
        return
    ts_col = _get_ts_column(tbl)
    order_clause = f"ORDER BY {ts_col} DESC" if ts_col else ""
    df = query_df(f"SELECT * FROM {tbl} {order_clause} LIMIT 1000")
    if df.empty:
        st.info("Orders table is empty.")
        return
    st.dataframe(df)


# Layout
prices_tab, trades_tab, staleness_tab, te_tab, hist_trades_tab, hist_orders_tab = st.tabs(
    [
        "Prices",
        "Recent Trades",
        "Staleness",
        "Tracking Error",
        "Historical Trades",
        "Historical Orders",
    ]
)

with prices_tab:
    render_prices()
with trades_tab:
    render_recent_trades()
with staleness_tab:
    render_staleness()
with te_tab:
    render_tracking_error()
with hist_trades_tab:
    render_historical_trades()
with hist_orders_tab:
    render_historical_orders()

st.sidebar.header("Config")
st.sidebar.write(f"Address: {HYPERLIQUID_ADDRESS}")
st.sidebar.write("Update env vars to change ClickHouse connection (CLICKHOUSE_HOST, CLICKHOUSE_USER, etc.)")
