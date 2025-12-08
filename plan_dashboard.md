# Dashboard Plan (Streamlit)

Goal: compact, actionable dashboard aligned with ipynb methodology and live ops.

## Tabs & Must-haves

1) Overview (KPIs)
   - KPIs: AUM (latest equity_usd/accountValue), 24h realized PnL, daily PnL %, latest te_daily & te_rolling_7d, last orchestrator run time.
   - Quick sparkline: last 14 days NAV.

2) PnL / Equity
   - Data: maicro_logs.live_account (prefer accountValue/totalNtlPos parsed from raw; fallback equity_usd).
   - Charts: NAV over time; daily returns (%); cumulative PnL.
   - Controls: date range selector; choose nav column (accountValue vs totalNtlPos vs equity_usd).

3) Tracking Error
   - Data: maicro_monitors.tracking_error.
   - Charts: te_daily line; te_rolling_7d line; cumulative tracking diff.
   - Controls: date range selector.

4) Positions
   - Data: maicro_monitors.positions_snapshots.
   - Logic: latest snapshot per coin; show qty, entryPx, positionValue, unrealizedPnl; compute weight = positionValue / sum(abs(positionValue)).
   - Views: table; top-N by abs USD; aggregate gross/net exposure.

5) Trades
   - Data: maicro_monitors.trades (recent N rows, e.g., 1000).
   - Metrics: count, notional, realized PnL (sum closedPnl), fees.
   - Filters: date range, coin.

6) System Health
   - Staleness: latest ts per key tables (prices, trades, orders, positions_snapshots, account_snapshots, tracking_error, candles).
   - Sync lag: compare max(ts) local vs remote for selected tables (if remote reachable via config); otherwise show N/A.
   - Orchestrator heartbeat: last run timestamp (from logs table if available, else max timestamp among ingested tables).
   - Highlight red if stale > threshold (e.g., 5m for trades/orders, 30m for candles, 24h for tracking_error).

## Implementation Notes

- Use cached queries with TTL (60s default). Keep queries minimal (LIMITs where possible).
- Reuse `_pick_table` and `_get_ts_column` utilities already in streamlit_main.py; extend TABLE_CANDIDATES if needed.
- Add small helper to parse marginSummary from live_account.raw (for accountValue/totalNtlPos) to mirror ipynb.
- Keep layout: tabs as in current file; extend each tab content rather than redesign.

### Performance guardrails
- Cache query results (`@st.cache_data(ttl=60)`) and avoid wide selects; project only needed columns.
- Parameterized date/coin filters to reduce dataset size; default to last 30–60 days.
- Use lazy aggregates (COUNT/SUM) for KPIs instead of moving all rows to Python when possible.
- Limit charts to last N points (e.g., 180 days) with downsampling if needed.

### Styling (clean, not default Streamlit)
- Inject a small CSS block via `st.markdown` once at top:
  - Set a neutral/ink palette (e.g., dark text on off-white, accent `#0f766e`, danger `#dc2626`).
  - Rounded cards, light shadows for KPI boxes, consistent padding.
  - Tighten table line-height and header weight; use monospace for numbers if needed.
- Define a simple utility class for KPI cards (flex row) and reuse across tabs.
- Keep backgrounds solid with subtle tinted header, avoid gradients/stock themes.
- Include a toggle for light/dim (two CSS blocks) if time permits.

## Stretch (later)
- Download CSV buttons per tab.
- Auto-refresh toggle.
- Compare live vs model weights (corr) using positions_jianan_v6 when desired.
