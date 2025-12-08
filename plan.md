# Maicro Monitors: Accountability Snapshot

Purpose: track live vs model performance (pnl + tracking error) and keep data flowing.

## 🚀 Current Status & Handoff (Dec 8, 2025)

Architecture (local-first): orchestrator ingests to local ClickHouse; sync pushes to remote; cron wraps both.

### ✅ Completed
- [x] Local DB ready (`maicrobot`).
- [x] Orchestrator (`scripts/orchestrate_monitors.py`).
- [x] Sync (`scripts/sync_to_remote.py`) + cron wrapper.
- [x] Sync cursors verified.
- [x] PnL calculator (`05_pnl_calculator/pnl_calculator.py`).
- [x] Tracking error calculator now ipynb-aligned; default 60d lookback; stores daily + 7d TE.

### 📋 Next Steps (The Plan)

1. Streamlit: add PnL/TE charts, positions (unrealized), trades.
2. Daily email: cron test with real `RESEND_API_KEY`.
3. System health tab: staleness + local/remote lag + last orchestrator run.
4. Docs: expand README after dashboard/health; add missing docstrings.
---
