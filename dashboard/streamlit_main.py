#!/usr/bin/env python3
"""Streamlit dashboard for Hyperliquid monitoring.
Covers: KPIs, PnL/equity, tracking error, positions, trades, system health.
"""
import datetime as dt
import json
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st
import os
import logging
import sys
from pathlib import Path

# Ensure the repository root is on sys.path so that `config` and `modules`
# can be imported regardless of how Streamlit is launched.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import HYPERLIQUID_ADDRESS, TABLE_CANDIDATES
from modules.clickhouse_client import first_existing, query_df, table_exists

# Set page config with mobile-friendly settings
st.set_page_config(
    page_title="Maicro Monitors",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"  # Changed to expanded for better mobile use
)

# Add comprehensive mobile-friendly CSS with clean styling
st.markdown("""
<style>
    /* Clean neutral palette */
    :root {
        --accent: #0f766e;
        --danger: #dc2626;
        --warning: #d97706;
        --success: #059669;
        --bg-card: #f8fafc;
        --border: #e2e8f0;
        --text-primary: #1e293b;
        --text-secondary: #64748b;
    }

    /* KPI Card styling */
    .kpi-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        margin-bottom: 0.5rem;
    }
    .kpi-label {
        font-size: 0.75rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.25rem;
    }
    .kpi-value {
        font-size: 1.5rem;
        font-weight: 600;
        color: var(--text-primary);
        font-family: 'SF Mono', 'Menlo', monospace;
    }
    .kpi-value.positive { color: var(--success); }
    .kpi-value.negative { color: var(--danger); }

    /* Status indicators */
    .status-ok { color: var(--success); }
    .status-warn { color: var(--warning); }
    .status-error { color: var(--danger); }

    /* Tighter table styling */
    .stDataFrame {
        font-size: 13px;
    }
    .stDataFrame td, .stDataFrame th {
        padding: 4px 8px !important;
        line-height: 1.4;
    }
    .stDataFrame th {
        font-weight: 600;
        background: var(--bg-card);
    }

    /* Mobile-friendly CSS */
    @media (max-width: 768px) {
        .stMarkdown, .stText, .stCaption { font-size: 14px !important; }
        .stTitle { font-size: 24px !important; }
        .stSubheader { font-size: 20px !important; }
        .main > .block-container {
            padding-top: 1rem;
            padding-left: 0.5rem;
            padding-right: 0.5rem;
        }
        .stDataFrame { font-size: 12px; }
        .element-container { width: 100% !important; overflow-x: auto; }
        .stTabs [data-baseweb="tab-list"] { overflow-x: auto; flex-wrap: wrap; font-size: 14px; }
        .stTabs [data-baseweb="tab"] { padding: 8px 12px !important; font-size: 14px; }
        .sidebar .sidebar-content { padding: 0.5rem; }
        .stSelectbox, .stNumberInput, .stTextInput, .stDateInput { font-size: 16px; }
        .stButton > button { font-size: 16px; padding: 10px 16px; min-height: 40px; }
        .kpi-value { font-size: 1.2rem; }
    }

    /* Tablet styling */
    @media (min-width: 769px) and (max-width: 1024px) {
        .stMarkdown, .stText, .stCaption { font-size: 15px; }
        .stTabs [data-baseweb="tab"] { padding: 10px 14px !important; }
    }

    /* Ensure dataframes are scrollable */
    .stDataFrame { display: block; overflow-x: auto; white-space: nowrap; max-width: 100%; }
    .element-container { width: 100%; max-width: 100%; }
    .stSelectbox > div > div, .stNumberInput > div > input,
    .stTextInput > div > input, .stDateInput > div > input { min-height: 40px; font-size: 16px; }
    .stColumn { margin-bottom: 1rem; }
    .stPlotContainer { min-height: 300px; }
    .sidebar .stHeading, .sidebar .stTitle { word-break: break-word; overflow-wrap: break-word; }
    h1 { font-size: 1.8rem !important; }
    h2 { font-size: 1.5rem !important; }
    h3 { font-size: 1.3rem !important; }
</style>
""", unsafe_allow_html=True)

st.title("📊 Maicro Monitors Dashboard")
st.caption("Hyperliquid trading accountability: KPIs, PnL, tracking error, positions, trades")


# ---------------------------
# Simple optional password gate
# ---------------------------
def _require_password(env_var_name: str = "MAICRO_DASH_PASSWORD") -> bool:
    """If env var is set, require a matching password input in the sidebar.
    Returns True when authentication is OK or no password is configured.
    """
    pw = os.getenv(env_var_name) or os.getenv("DASHBOARD_PASSWORD")
    if not pw:
        # no password configured — warn but allow access
        st.sidebar.warning("Dashboard is running without a password. Set env var MAICRO_DASH_PASSWORD to secure it.")
        return True

    # Use Session State to persist successful auth
    if "maicro_dashboard_authenticated" not in st.session_state:
        st.session_state["maicro_dashboard_authenticated"] = False

    if not st.session_state["maicro_dashboard_authenticated"]:
        # Show password input on sidebar
        with st.sidebar.form(key="auth_form"):
            user_pw = st.text_input("Enter dashboard password", type="password", key="pw_input")
            submitted = st.form_submit_button("Authenticate")
            if submitted:
                if user_pw == pw:
                    st.session_state["maicro_dashboard_authenticated"] = True
                    st.sidebar.success("Authenticated")
                    logging.getLogger(__name__).info("Streamlit Dashboard authenticated successfully for this session")
                else:
                    st.sidebar.error("Incorrect password; try again.")
        # Stop the script from rendering rest of the app until authenticated
        if not st.session_state["maicro_dashboard_authenticated"]:
            st.stop()
    return True


# enforce auth gate before rendering any content
_require_password()


def kpi_card(label: str, value: str, delta: Optional[str] = None, status: str = "neutral"):
    """Render a styled KPI card."""
    value_class = "positive" if status == "positive" else "negative" if status == "negative" else ""
    delta_html = f'<div style="font-size:0.8rem;color:{"#059669" if status=="positive" else "#dc2626" if status=="negative" else "#64748b"}">{delta}</div>' if delta else ""
    return f'''<div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value {value_class}">{value}</div>
        {delta_html}
    </div>'''


def parse_margin_summary(raw):
    """Parse marginSummary from raw JSON field."""
    if pd.isna(raw) or raw is None:
        return {}
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return data.get('marginSummary', {})
    except Exception:
        return {}


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


@st.cache_data(ttl=60)
def load_live_account_data(lookback_days: int = 60):
    """Load live account data with parsed margin summary."""
    try:
        df = query_df(f"""
            SELECT ts, equity_usd, raw
            FROM maicro_logs.live_account
            WHERE ts >= now() - INTERVAL {lookback_days} DAY
            ORDER BY ts
        """)
        if df.empty:
            return df
        # Parse margin summary
        margin_data = df['raw'].apply(parse_margin_summary)
        df['accountValue'] = margin_data.apply(lambda x: float(x.get('accountValue', 0)) if x else None)
        df['totalNtlPos'] = margin_data.apply(lambda x: float(x.get('totalNtlPos', 0)) if x else None)
        df = df.drop(columns=['raw'])
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_tracking_error_data(lookback_days: int = 60):
    """Load tracking error data."""
    tbl = _pick_table("tracking_error")
    if not tbl:
        return pd.DataFrame()
    try:
        return query_df(f"""
            SELECT date, te_daily, te_rolling_7d
            FROM {tbl}
            WHERE date >= toDate(now() - INTERVAL {lookback_days} DAY)
            ORDER BY date
        """)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_positions_data():
    """Load latest positions snapshot with computed weights."""
    tbl = _pick_table("positions")
    if not tbl:
        return pd.DataFrame()
    try:
        df = query_df(f"""
            SELECT coin, szi as qty, entryPx, positionValue, unrealizedPnl, timestamp
            FROM {tbl}
            WHERE (coin, timestamp) IN (
                SELECT coin, max(timestamp) FROM {tbl} GROUP BY coin
            )
            ORDER BY abs(positionValue) DESC
        """)
        if not df.empty and 'positionValue' in df.columns:
            total_abs = df['positionValue'].abs().sum()
            df['weight'] = df['positionValue'] / total_abs if total_abs > 0 else 0
            df['weight_pct'] = df['weight'] * 100
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_trades_summary(lookback_days: int = 30):
    """Load trade summary metrics."""
    tbl = _pick_table("trades")
    if not tbl:
        return {}
    try:
        df = query_df(f"""
            SELECT
                count() as trade_count,
                sum(abs(sz * px)) as notional,
                sum(closedPnl) as realized_pnl,
                sum(fee) as total_fees
            FROM {tbl}
            WHERE time >= now() - INTERVAL {lookback_days} DAY
        """)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception:
        return {}


@st.cache_data(ttl=60)
def load_24h_pnl():
    """Load 24h realized PnL."""
    tbl = _pick_table("trades")
    if not tbl:
        return None
    try:
        df = query_df(f"""
            SELECT sum(closedPnl) as pnl_24h
            FROM {tbl}
            WHERE time >= now() - INTERVAL 24 HOUR
        """)
        return df['pnl_24h'].iloc[0] if not df.empty else None
    except Exception:
        return None


@st.cache_data(ttl=300)
def load_model_backtest_data(lookback_days: int = 180):
    """Load model backtest daily returns (T-1, T-2) and weights.

    Uses positions_jianan_v6 as the model weights and maicro_monitors.candles
    (interval='1d') for market closes.
    """
    # Look back a bit further to ensure we have enough price history
    end_date = pd.Timestamp.now().normalize().date()
    start_date = end_date - pd.Timedelta(days=lookback_days + 5)
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    # Load earliest target per (date, symbol) with finite, non-zero weight
    targets = query_df(
        """
        SELECT date, symbol, weight, pred_ret, inserted_at
        FROM (
            SELECT date, symbol, weight, pred_ret, inserted_at
            FROM maicro_logs.positions_jianan_v6
            WHERE date BETWEEN %(start)s AND %(end)s
              AND weight IS NOT NULL AND isFinite(weight) AND weight != 0
            ORDER BY date, symbol, inserted_at
            LIMIT 1 BY date, symbol
        )
        ORDER BY date, symbol
        """,
        params={"start": start_str, "end": end_str},
    )
    if targets.empty:
        return {
            "returns_t1": pd.Series(dtype=float),
            "returns_t2": pd.Series(dtype=float),
            "weights": pd.DataFrame(),
            "targets": pd.DataFrame(),
        }

    targets["date"] = pd.to_datetime(targets["date"])
    targets["symbol"] = targets["symbol"].str.upper().str.strip()

    # Wide weights matrix: date x symbol
    weights = (
        targets.pivot(index="date", columns="symbol", values="weight")
        .sort_index()
    )
    if weights.empty:
        return {
            "returns_t1": pd.Series(dtype=float),
            "returns_t2": pd.Series(dtype=float),
            "weights": pd.DataFrame(),
            "targets": targets,
        }

    first_date = weights.index.min()
    last_date = weights.index.max()
    if pd.isna(first_date) or pd.isna(last_date):
        return {
            "returns_t1": pd.Series(dtype=float),
            "returns_t2": pd.Series(dtype=float),
            "weights": weights,
            "targets": targets,
        }

    # Load daily closes from candles (interval='1d')
    # Start a few days earlier to be safe for return calculation.
    start_ts = (first_date - pd.Timedelta(days=3)).strftime("%Y-%m-%d 00:00:00")
    prices_raw = query_df(
        """
        SELECT toDate(ts) AS date, coin, close
        FROM maicro_monitors.candles
        WHERE ts >= toDateTime(%(start_ts)s)
          AND interval = '1d'
        ORDER BY date, coin
        """,
        params={"start_ts": start_ts},
    )
    if prices_raw.empty:
        return {
            "returns_t1": pd.Series(dtype=float),
            "returns_t2": pd.Series(dtype=float),
            "weights": weights,
            "targets": targets,
        }

    prices_raw["date"] = pd.to_datetime(prices_raw["date"])
    prices_raw["coin"] = prices_raw["coin"].str.upper().str.strip()
    price_pivot = (
        prices_raw.pivot(index="date", columns="coin", values="close")
        .sort_index()
    )
    if price_pivot.empty:
        return {
            "returns_t1": pd.Series(dtype=float),
            "returns_t2": pd.Series(dtype=float),
            "weights": weights,
            "targets": targets,
        }

    # Forward daily returns: close_{t+1} / close_t - 1
    market_returns = price_pivot.shift(-1) / price_pivot - 1.0

    # Align on common dates and symbol universe
    common_idx = weights.index.intersection(market_returns.index)
    if common_idx.empty:
        return {
            "returns_t1": pd.Series(dtype=float),
            "returns_t2": pd.Series(dtype=float),
            "weights": weights,
            "targets": targets,
        }

    weights = weights.loc[common_idx].fillna(0.0)
    market_returns = (
        market_returns.loc[common_idx]
        .reindex(columns=weights.columns)
        .fillna(0.0)
    )

    # Strategy returns with T-1 and T-2 execution lags
    returns_t1 = (market_returns * weights.shift(1).fillna(0.0)).sum(axis=1)
    returns_t2 = (market_returns * weights.shift(2).fillna(0.0)).sum(axis=1)

    # Drop initial NaNs created by shifting (if any)
    returns_t1 = returns_t1.dropna()
    returns_t2 = returns_t2.dropna()

    return {
        "returns_t1": returns_t1,
        "returns_t2": returns_t2,
        "weights": weights,
        "targets": targets,
    }


def _compute_return_metrics(daily_returns: pd.Series) -> dict:
    """Compute Sharpe and related stats from daily returns (in decimal)."""
    r = daily_returns.dropna()
    if r.empty:
        return {
            "avg_daily": np.nan,
            "vol_daily": np.nan,
            "ann_return": np.nan,
            "ann_vol": np.nan,
            "sharpe": np.nan,
            "n_days": 0,
        }

    avg_daily = r.mean()
    vol_daily = r.std()
    ann_factor = np.sqrt(252.0)
    ann_return = (1.0 + avg_daily) ** 252 - 1.0
    ann_vol = vol_daily * ann_factor
    sharpe = (avg_daily / vol_daily) * ann_factor if vol_daily > 0 else np.nan

    return {
        "avg_daily": avg_daily,
        "vol_daily": vol_daily,
        "ann_return": ann_return,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "n_days": int(len(r)),
    }


# Staleness thresholds (in minutes)
STALENESS_THRESHOLDS = {
    "trades": 5,
    "orders": 5,
    "prices": 30,
    "tracking_error": 1440,  # 24h
    "positions": 10,
    "account": 10,
}


def render_tracking_error():
    """Render Tracking Error tab per plan."""
    st.subheader("Tracking Error")

    # Date range selector
    col_date1, col_date2 = st.columns([1, 1])
    with col_date1:
        start_date = st.date_input(
            "Start date",
            value=pd.Timestamp.now() - pd.Timedelta(days=30),
            help="Select start date for tracking error data",
            key="te_start"
        )
    with col_date2:
        end_date = st.date_input(
            "End date",
            value=pd.Timestamp.now().date(),
            help="Select end date for tracking error data",
            key="te_end"
        )

    if pd.Timestamp(start_date) > pd.Timestamp(end_date):
        st.error("Error: End date must be after start date.")
        return

    tbl = _pick_table("tracking_error")
    if not tbl:
        st.warning("No tracking_error table found; expected maicro_monitors.tracking_error.")
        return

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    df = query_df(f"""
        SELECT * FROM {tbl}
        WHERE date BETWEEN '{start_date_str}' AND '{end_date_str}'
        ORDER BY date
    """)

    if df.empty:
        st.info("No tracking error data available for the selected date range.")
        return

    # TE Daily line chart
    if 'te_daily' in df.columns:
        st.subheader("Daily Tracking Error")
        chart_df = df.set_index("date")[['te_daily']].dropna()
        st.line_chart(chart_df, use_container_width=True)

    # TE Rolling 7d line chart
    if 'te_rolling_7d' in df.columns:
        st.subheader("Rolling 7D Tracking Error")
        chart_df = df.set_index("date")[['te_rolling_7d']].dropna()
        st.line_chart(chart_df, use_container_width=True)

    # Cumulative Tracking Difference
    if 'te_daily' in df.columns:
        st.subheader("Cumulative Tracking Difference")
        df['cum_te'] = (1 + df['te_daily'].fillna(0)).cumprod() - 1
        chart_df = df.set_index("date")[['cum_te']]
        st.line_chart(chart_df, use_container_width=True)

    # Data table
    st.markdown("---")
    st.subheader("Raw Data")
    if len(df) > 20:
        st.dataframe(df.tail(20), use_container_width=True)
        with st.expander("View all data"):
            st.dataframe(df, use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)


def render_overview():
    """Render Overview tab with all KPIs per plan."""
    st.subheader("Overview (KPIs)")

    # Load data using cached functions
    account_df = load_live_account_data(60)
    te_df = load_tracking_error_data(60)
    pnl_24h = load_24h_pnl()

    # Extract latest values
    aum = None
    daily_pnl_pct = None
    last_update = None
    te_daily = None
    te_rolling_7d = None

    if not account_df.empty:
        latest = account_df.iloc[-1]
        aum = latest.get('accountValue') or latest.get('equity_usd')
        last_update = str(latest.get('ts', ''))[:16]
        # Calculate daily PnL % if we have enough data
        if len(account_df) >= 2:
            today_nav = account_df.iloc[-1].get('accountValue') or account_df.iloc[-1].get('equity_usd')
            yesterday_nav = account_df.iloc[-2].get('accountValue') or account_df.iloc[-2].get('equity_usd')
            if today_nav and yesterday_nav and yesterday_nav != 0:
                daily_pnl_pct = ((today_nav - yesterday_nav) / yesterday_nav) * 100

    if not te_df.empty:
        te_latest = te_df.iloc[-1]
        te_daily = te_latest.get('te_daily')
        te_rolling_7d = te_latest.get('te_rolling_7d')

    # Display KPIs in styled cards
    col1, col2, col3 = st.columns(3)
    with col1:
        aum_str = f"${float(aum):,.0f}" if aum else "N/A"
        st.markdown(kpi_card("AUM (USD)", aum_str), unsafe_allow_html=True)
    with col2:
        pnl_str = f"${float(pnl_24h):,.2f}" if pnl_24h else "N/A"
        status = "positive" if pnl_24h and pnl_24h > 0 else "negative" if pnl_24h and pnl_24h < 0 else "neutral"
        st.markdown(kpi_card("24h Realized PnL", pnl_str, status=status), unsafe_allow_html=True)
    with col3:
        pct_str = f"{daily_pnl_pct:+.2f}%" if daily_pnl_pct is not None else "N/A"
        status = "positive" if daily_pnl_pct and daily_pnl_pct > 0 else "negative" if daily_pnl_pct and daily_pnl_pct < 0 else "neutral"
        st.markdown(kpi_card("Daily PnL %", pct_str, status=status), unsafe_allow_html=True)

    col4, col5, col6 = st.columns(3)
    with col4:
        te_str = f"{float(te_daily):.4f}" if te_daily is not None else "N/A"
        st.markdown(kpi_card("TE Daily", te_str), unsafe_allow_html=True)
    with col5:
        te7_str = f"{float(te_rolling_7d):.4f}" if te_rolling_7d is not None else "N/A"
        st.markdown(kpi_card("TE Rolling 7D", te7_str), unsafe_allow_html=True)
    with col6:
        st.markdown(kpi_card("Last Update", last_update or "N/A"), unsafe_allow_html=True)

    # NAV sparkline (last 14 days)
    st.markdown("---")
    st.subheader("NAV Trend (Last 14 Days)")
    if not account_df.empty:
        # Resample to daily for cleaner chart
        chart_df = account_df.copy()
        chart_df['ts'] = pd.to_datetime(chart_df['ts'])
        chart_df = chart_df.set_index('ts')
        nav_col = 'accountValue' if 'accountValue' in chart_df.columns and chart_df['accountValue'].notna().any() else 'equity_usd'
        daily_nav = chart_df[nav_col].resample('1D').last().dropna().tail(14)
        if not daily_nav.empty:
            st.line_chart(daily_nav, use_container_width=True, height=250)
        else:
            st.info("Not enough data for NAV chart")
    else:
        st.info("No account data available")


def render_pnl_equity():
    """Render PnL / Equity tab per plan."""
    st.subheader("PnL / Equity")

    # Controls
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        start_date = st.date_input("Start", value=pd.Timestamp.now() - pd.Timedelta(days=30), key="pnl_start")
    with col2:
        end_date = st.date_input("End", value=pd.Timestamp.now(), key="pnl_end")
    with col3:
        nav_col = st.selectbox("NAV Column", ["accountValue", "totalNtlPos", "equity_usd"], key="nav_col")

    if start_date > end_date:
        st.error("End date must be after start date.")
        return

    # Load data
    lookback = (pd.Timestamp.now().date() - start_date).days + 5
    df = load_live_account_data(lookback)
    if df.empty:
        st.warning("No account data available")
        return

    df['ts'] = pd.to_datetime(df['ts'])
    df = df[(df['ts'].dt.date >= start_date) & (df['ts'].dt.date <= end_date)]

    if df.empty:
        st.info("No data in selected range")
        return

    # Use selected NAV column
    if nav_col not in df.columns or df[nav_col].isna().all():
        nav_col = 'equity_usd'  # fallback

    df = df.set_index('ts').sort_index()

    # NAV over time chart
    st.subheader("NAV Over Time")
    st.line_chart(df[nav_col].dropna(), use_container_width=True)

    # Daily returns
    daily_nav = df[nav_col].resample('1D').last().dropna()
    daily_returns = daily_nav.pct_change().dropna() * 100

    if len(daily_returns) > 0:
        st.subheader("Daily Returns (%)")
        st.bar_chart(daily_returns, use_container_width=True)

        # Cumulative PnL
        st.subheader("Cumulative PnL")
        cum_returns = (1 + daily_returns / 100).cumprod() - 1
        st.line_chart(cum_returns * 100, use_container_width=True)

        # Summary stats
        st.markdown("**Summary Statistics**")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Avg Daily Return", f"{daily_returns.mean():.3f}%")
        with col2:
            st.metric("Daily Volatility", f"{daily_returns.std():.3f}%")
        with col3:
            sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252) if daily_returns.std() != 0 else 0
            st.metric("Sharpe (Ann.)", f"{sharpe:.2f}")
        with col4:
            total_return = cum_returns.iloc[-1] * 100 if len(cum_returns) > 0 else 0
            st.metric("Total Return", f"{total_return:.2f}%")


def render_backtest():
    """Render Backtest tab: T-1/T-2 equity curves + model holdings."""
    st.subheader("Model Backtest: T-1 / T-2")

    # Toggles (stacked vertically for better mobile layout)
    show_curves = st.checkbox(
        "Show backtest curves",
        value=True,
        key="bt_show_curves",
        help="Toggle T-1 and T-2 equity curves and summary stats.",
    )
    show_positions = st.checkbox(
        "Show model holdings for date",
        value=True,
        key="bt_show_positions",
        help="Toggle the table of model-implied holdings for a selected date.",
    )

    # Lookback configuration
    lookback_days = st.slider(
        "Lookback window (days)",
        min_value=30,
        max_value=365,
        value=180,
        step=30,
        help="Controls how many days of backtest history to load from ClickHouse.",
        key="bt_lookback_days",
    )

    data = load_model_backtest_data(lookback_days)
    ret_t1: pd.Series = data["returns_t1"]
    ret_t2: pd.Series = data["returns_t2"]
    weights: pd.DataFrame = data["weights"]
    targets: pd.DataFrame = data["targets"]

    if show_curves:
        st.markdown("### Equity Curves (Index, base 100)")
        if ret_t1.empty and ret_t2.empty:
            st.warning("No backtest data available. Check maicro_logs.positions_jianan_v6 and maicro_monitors.candles.")
        else:
            curves = pd.DataFrame()
            if not ret_t1.empty:
                curves["T-1 equity"] = (1.0 + ret_t1).cumprod() * 100.0
            if not ret_t2.empty:
                curves["T-2 equity"] = (1.0 + ret_t2).cumprod() * 100.0

            st.line_chart(curves, use_container_width=True)

            # Metrics
            st.markdown("### Backtest Statistics")
            col1, col2 = st.columns(2)
            if not ret_t1.empty:
                m1 = _compute_return_metrics(ret_t1)
                with col1:
                    st.markdown("**T-1 (weights lag 1 day)**")
                    st.metric("Sharpe (Ann.)", f"{m1['sharpe']:.2f}" if not np.isnan(m1["sharpe"]) else "N/A")
                    st.metric("Ann. Return", f"{m1['ann_return']*100:.2f}%" if not np.isnan(m1["ann_return"]) else "N/A")
                    st.metric("Ann. Vol", f"{m1['ann_vol']*100:.2f}%" if not np.isnan(m1["ann_vol"]) else "N/A")
                    st.caption(f"N daily returns: {m1['n_days']}")
            if not ret_t2.empty:
                m2 = _compute_return_metrics(ret_t2)
                with col2:
                    st.markdown("**T-2 (weights lag 2 days)**")
                    st.metric("Sharpe (Ann.)", f"{m2['sharpe']:.2f}" if not np.isnan(m2["sharpe"]) else "N/A")
                    st.metric("Ann. Return", f"{m2['ann_return']*100:.2f}%" if not np.isnan(m2["ann_return"]) else "N/A")
                    st.metric("Ann. Vol", f"{m2['ann_vol']*100:.2f}%" if not np.isnan(m2["ann_vol"]) else "N/A")
                    st.caption(f"N daily returns: {m2['n_days']}")

    if show_positions:
        st.markdown("---")
        st.markdown("### Model Holdings for Selected Date")

        if targets.empty:
            st.info("No model target data available in positions_jianan_v6 for this window.")
            return

        # Available dates from targets
        available_dates = sorted(targets["date"].dt.date.unique())
        if not available_dates:
            st.info("No dates with model targets.")
            return

        default_date = available_dates[-1]
        selected_date = st.selectbox(
            "Backtest holdings date",
            options=available_dates,
            index=len(available_dates) - 1,
            format_func=lambda d: d.strftime("%Y-%m-%d"),
            key="bt_holdings_date",
        )

        # Filter targets for the selected date
        mask = targets["date"].dt.date == selected_date
        day_targets = targets.loc[mask].copy()
        if day_targets.empty:
            st.info("No holdings for the selected date.")
            return

        day_targets["abs_weight"] = day_targets["weight"].abs()
        day_targets.sort_values("abs_weight", ascending=False, inplace=True)

        # Display top holdings by absolute weight
        display_cols = ["symbol", "weight", "pred_ret", "abs_weight"]
        display_cols = [c for c in display_cols if c in day_targets.columns]
        top_view = day_targets[display_cols].head(25)
        st.dataframe(top_view, use_container_width=True)

        if len(day_targets) > 25:
            with st.expander("View all holdings for this date"):
                st.dataframe(
                    day_targets[display_cols],
                    use_container_width=True,
                )


def render_positions():
    """Render Positions tab per plan."""
    st.subheader("Current Positions")

    df = load_positions_data()
    if df.empty:
        st.warning("No positions data available. Check maicro_monitors.positions_snapshots table.")
        return

    # Display table
    display_cols = ['coin', 'qty', 'entryPx', 'positionValue', 'unrealizedPnl', 'weight_pct']
    display_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(df[display_cols].head(20), use_container_width=True)

    # Aggregate metrics
    st.markdown("---")
    st.subheader("Exposure Summary")
    col1, col2, col3 = st.columns(3)

    gross_exposure = df['positionValue'].abs().sum() if 'positionValue' in df.columns else 0
    net_exposure = df['positionValue'].sum() if 'positionValue' in df.columns else 0
    unrealized = df['unrealizedPnl'].sum() if 'unrealizedPnl' in df.columns else 0

    with col1:
        st.metric("Gross Exposure", f"${gross_exposure:,.0f}")
    with col2:
        st.metric("Net Exposure", f"${net_exposure:,.0f}")
    with col3:
        st.metric("Unrealized PnL", f"${unrealized:,.2f}")

    # Top positions by absolute value
    if len(df) > 5:
        st.subheader("Top 5 Positions (by |Value|)")
        top5 = df.nlargest(5, df['positionValue'].abs() if 'positionValue' in df.columns else 'qty')
        st.bar_chart(top5.set_index('coin')['positionValue'], use_container_width=True)


def render_trades_tab():
    """Render Trades tab with metrics per plan."""
    st.subheader("Trades")

    # Controls
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        start_date = st.date_input("Start", value=pd.Timestamp.now() - pd.Timedelta(days=7), key="trades_start")
    with col2:
        end_date = st.date_input("End", value=pd.Timestamp.now(), key="trades_end")

    tbl = _pick_table("trades")
    if not tbl:
        st.warning("No trades table found.")
        return

    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    # Load aggregated metrics
    try:
        metrics_df = query_df(f"""
            SELECT
                count() as trade_count,
                sum(abs(sz * px)) as notional,
                sum(closedPnl) as realized_pnl,
                sum(fee) as total_fees
            FROM {tbl}
            WHERE toDate(time) BETWEEN '{start_str}' AND '{end_str}'
        """)
        if not metrics_df.empty:
            m = metrics_df.iloc[0]
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Trade Count", f"{int(m.get('trade_count', 0)):,}")
            with col2:
                st.metric("Notional", f"${m.get('notional', 0):,.0f}")
            with col3:
                pnl = m.get('realized_pnl', 0)
                st.metric("Realized PnL", f"${pnl:,.2f}", delta_color="normal" if pnl >= 0 else "inverse")
            with col4:
                st.metric("Total Fees", f"${m.get('total_fees', 0):,.2f}")
    except Exception as e:
        st.warning(f"Could not load trade metrics: {e}")

    st.markdown("---")

    # Coin filter
    try:
        coins_df = query_df(f"SELECT DISTINCT coin FROM {tbl} ORDER BY coin LIMIT 100")
        coins = ["All"] + coins_df['coin'].tolist() if not coins_df.empty else ["All"]
    except:
        coins = ["All"]

    with col3:
        coin_filter = st.selectbox("Coin", coins, key="trades_coin")

    # Load recent trades
    coin_clause = f"AND coin = '{coin_filter}'" if coin_filter != "All" else ""
    ts_col = _get_ts_column(tbl) or "time"

    df = query_df(f"""
        SELECT * FROM {tbl}
        WHERE toDate({ts_col}) BETWEEN '{start_str}' AND '{end_str}'
        {coin_clause}
        ORDER BY {ts_col} DESC
        LIMIT 500
    """)

    if df.empty:
        st.info("No trades in selected range.")
        return

    # Display table
    if len(df) > 20:
        st.dataframe(df.head(20), use_container_width=True)
        with st.expander("View more"):
            st.dataframe(df.head(100), use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)


def render_system_health():
    """Render System Health tab with staleness thresholds per plan."""
    st.subheader("System Health")

    now = pd.Timestamp.now()
    rows = []

    sources = ["prices", "trades", "orders", "positions", "account", "tracking_error"]

    for kind in sources:
        tbl = _pick_table(kind)
        if not tbl:
            rows.append({"source": kind, "table": "Not found", "latest_ts": None, "age_min": None, "status": "❓"})
            continue

        ts_col = _get_ts_column(tbl)
        if not ts_col:
            rows.append({"source": kind, "table": tbl, "latest_ts": None, "age_min": None, "status": "❓"})
            continue

        try:
            sql = f"SELECT max({ts_col}) AS latest FROM {tbl}"
            latest_df = query_df(sql)
            latest = latest_df["latest"].iloc[0] if not latest_df.empty else None

            if latest:
                latest_ts = pd.to_datetime(latest)
                age_min = (now - latest_ts).total_seconds() / 60
                threshold = STALENESS_THRESHOLDS.get(kind, 60)

                if age_min > threshold * 2:
                    status = "🔴 Stale"
                elif age_min > threshold:
                    status = "🟡 Warning"
                else:
                    status = "🟢 OK"

                rows.append({
                    "source": kind,
                    "table": tbl,
                    "latest_ts": str(latest)[:19],
                    "age_min": f"{age_min:.0f}",
                    "threshold_min": threshold,
                    "status": status
                })
            else:
                rows.append({"source": kind, "table": tbl, "latest_ts": "N/A", "age_min": "N/A", "status": "❓"})
        except Exception as e:
            rows.append({"source": kind, "table": tbl, "latest_ts": f"Error: {e}", "age_min": "N/A", "status": "🔴"})

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True)

    # Legend
    st.markdown("""
    **Status Legend:**
    - 🟢 OK: Within threshold
    - 🟡 Warning: 1-2x threshold
    - 🔴 Stale: >2x threshold
    """)

    st.button("🔄 Refresh", on_click=st.cache_data.clear)


# Layout - tabs aligned with plan_dashboard.md
overview_tab, pnl_tab, backtest_tab, te_tab, positions_tab, trades_tab, health_tab = st.tabs(
    [
        "📊 Overview",
        "💰 PnL/Equity",
        "📈 Backtest",
        "📏 Tracking Error",
        "📦 Positions",
        "🔄 Trades",
        "🏥 System Health",
    ]
)

with overview_tab:
    render_overview()
with pnl_tab:
    render_pnl_equity()
with backtest_tab:
    render_backtest()
with te_tab:
    render_tracking_error()
with positions_tab:
    render_positions()
with trades_tab:
    render_trades_tab()
with health_tab:
    render_system_health()

st.sidebar.header("Config")
st.sidebar.write(f"**Address:** `{HYPERLIQUID_ADDRESS[:10]}...`")
st.sidebar.markdown("---")
st.sidebar.caption("Update env vars for ClickHouse connection")
