# Maicro Monitors: Hyperliquid Trading Accountability System

**Purpose**: Track lead trader execution quality by comparing target weights vs actual positions, measure tracking error over N days, and provide comprehensive monitoring infrastructure.

---

## 🚀 Current Status & Handoff (Dec 8, 2025)

**Architecture Shift**: We have moved from individual scripts to a consolidated **Local-First** architecture.
1.  **Ingestion**: `scripts/orchestrate_monitors.py` runs locally on `chenlin04`. It collects data (Account, Positions, Trades, Orders, Funding, Ledger, OHLCV) and writes to the **Local ClickHouse** instance.
2.  **Synchronization**: `scripts/sync_to_remote.py` runs immediately after ingestion. It pushes new data from **Local ClickHouse** to **Remote Cloud ClickHouse** (Maicro Cloud).
3.  **Orchestration**: `scripts/run_monitors_and_sync.sh` wraps both steps and is scheduled via cron.

### ✅ Completed
- [x] **Local DB Setup**: User `maicrobot` created, schema initialized locally.
- [x] **Orchestrator**: Consolidated all monitors into `scripts/orchestrate_monitors.py`.
- [x] **Sync Script**: `scripts/sync_to_remote.py` implements incremental sync (Local -> Remote) for `maicro_monitors`, `maicro_logs`, `binance`, and `hyperliquid` tables.
- [x] **Cron Setup**: `scripts/generate_cron.py` generates the crontab entry for the combined workflow.
- [x] **Downward Sync Verified**: Confirmed `scripts/sync_to_remote.py` uses `max(time_col)` cursors and successfully pushes new data to Remote ClickHouse.
- [x] **PnL Calculator**: Implemented `05_pnl_calculator/pnl_calculator.py` handling realized PnL, funding, and unrealized PnL from snapshots.
- [x] **Tracking Error Calculator**: Updated `05_tracking_error/tracking_error_calculator.py` to mirror the ipynb (uses `maicro_logs.live_account`, `maicro_logs.positions_jianan_v6`, `maicro_monitors.candles` forward returns) with a default 60-day lookback (`--lookback` to override), storing Daily TE and 7d rolling into `maicro_monitors.tracking_error`.

### 📋 Next Steps (The Plan)

1. **Refine Dashboard (Streamlit)**
   - Add PnL + TE charts (Daily & 7d), positions with unrealized PnL, and trade history to `06_dashboards/streamlit_main.py`.
2. **Finalize Summary Email**
   - End-to-end test `reports/daily_summary_email.py` via cron with real env vars (`RESEND_API_KEY`).
3. **System Health Monitoring**
   - New dashboard tab for table staleness and local-vs-remote sync lag; surface orchestrator last-run status.
4. **Documentation & Handoff**
   - Expand README after dashboard/health updates (usage, cron ops, troubleshooting); ensure docstrings are present where missing.

---

## 📂 Project Structure (Updated)

```
maicro_monitors/
├── plan.md                           # This document
├── config/
│   ├── settings.py                   # Dual Config: CLICKHOUSE_LOCAL_CONFIG & CLICKHOUSE_REMOTE_CONFIG
│   └── ...
├── scripts/
│   ├── orchestrate_monitors.py       # MAIN INGESTOR
│   ├── sync_to_remote.py             # MAIN SYNC
│   ├── run_monitors_and_sync.sh      # Wrapper for Cron
│   └── ...
├── modules/
│   ├── hyperliquid_client.py
│   ├── clickhouse_client.py
│   └── buffer_manager.py
├── reports/
│   └── daily_summary_email.py        # Daily Email Report
├── 05_pnl_calculator/
│   └── pnl_calculator.py             # PnL Logic
├── 05_tracking_error/
│   └── tracking_error_calculator.py  # TE Logic
└── 06_dashboards/
    └── streamlit_main.py             # Dashboard
```

---

## 🛑 Deprecated / Legacy (Reference Only)

*The following modules have been consolidated into `scripts/orchestrate_monitors.py` but kept for logic reference.*

- `01_trade_logger/`
- `02_order_monitor/`
- `03_ohlcv_puller/`
- `04_data_feed_latency/`

---

## Executive Summary (Original)

⚠️ Credentials: swap to non-admin ClickHouse user. The current defaults in `config/settings.py` include a privileged user; replace with a least-privilege account (read-only for dashboards, scoped write for ingesters) and override via env vars. Also consider primary/secondary endpoints for redundancy.

This system monitors Hyperliquid perpetual trading operations through:
1. Real-time trade logging to ClickHouse
2. Order state monitoring
3. Daily OHLCV price pulls
4. Data feed latency testing
5. **Tracking error estimation**: comparing `target_weights` (model signals) vs `actual_weights` (live positions)
