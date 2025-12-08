# Maicro Monitors

Monitors for Hyperliquid trading built around ClickHouse:

- Ingests Hyperliquid account/positions/trades/funding/ledger/candles.
- Stores data in `maicro_monitors.*` on chenlin + ClickHouse Cloud.
- Computes tracking error & PnL.
- Provides a Streamlit dashboard and scheduled emails/alerts.

Architecture is “buffer first, then flush”:

> Hyperliquid HTTP → `data/buffer/*.parquet` → ClickHouse (chenlin + cloud)

## Quickstart

```bash
pip install -r requirements.txt

# 1) Create core tables (maicro_monitors.*) on chenlin
python3 scripts/init_db.py

# 2) Run a one-off ingest and flush
python3 scheduled_processes/scheduled_ping_hyperliquid.py   # buffer-only, talks to Hyperliquid
python3 scheduled_processes/flush_hyperliquid_buffers.py     # writes to chenlin + ClickHouse Cloud

# 3) Compute tracking error & PnL (optional one-off)
python3 05_tracking_error/tracking_error_calculator.py --lookback 60
python3 05_pnl_calculator/pnl_calculator.py

# 4) Start the dashboard
export MAICRO_DASH_PASSWORD="MyStrongPassword"
dashboard/run_dashboard.sh

# 5) Install cron (optional)
python3 scripts/register_cron.py    # appends maicro_monitors jobs to your crontab
```

> ⚠️ Note: Local archives or deprecated folders are intentionally ignored from commits/pushes.
> The directories `maicro_ignore_old/` and `deprecated/` are kept out of the repository and won't be pushed.

## Dashboard Security

The Streamlit dashboard can be password-protected using the `MAICRO_DASH_PASSWORD` environment variable. When set, a password prompt appears in the dashboard's sidebar and the main content is blocked until authentication succeeds.

To run the dashboard and require a password use:

```bash
export MAICRO_DASH_PASSWORD="MyStrongPassword"
dashboard/run_dashboard.sh
```

To run without requiring a password (for quick local debugging):

```bash
export FORCE_NO_PASSWORD=1
dashboard/run_dashboard.sh
```

## Scheduled Processes

All cron suggestions live in `scheduled_processes/cron.md`. The key jobs:

1. **Hyperliquid ingest (buffer + flush)**

   - `scheduled_processes/scheduled_ping_hyperliquid.py` (hourly, buffer‑only)  
     Polls Hyperliquid and writes Parquet files under `data/buffer/` for:
     account, positions, trades, orders, funding, ledger, candles.

   - `scheduled_processes/flush_hyperliquid_buffers.py` (every 3h)  
     Reads `data/buffer/*.parquet` and inserts into:
       - chenlin ClickHouse (`CLICKHOUSE_LOCAL_CONFIG`)
       - ClickHouse Cloud (`CLICKHOUSE_REMOTE_CONFIG`)
     targets: `maicro_monitors.account_snapshots`, `positions_snapshots`,
     `trades`, `orders`, `funding_payments`, `ledger_updates`, `candles`.

2. **Cloud → chenlin down‑sync (every 6h)**

   - `scheduled_processes/pull_data_downward_from_cloud.py`  
     Pulls from ClickHouse Cloud to chenlin for:
       - `hyperliquid.*`
       - `maicro_logs.*` (excluding deprecated `positions_jianan*` v1–v5)
       - `binance.*`  
     Uses per‑table cursor columns (e.g. `ts`, `inserted_at`) for incremental sync.

3. **Daily Emails (optional)**

   - `scheduled_processes/emails/daily/targets_vs_actuals_daily.py`  
     Target‑vs‑actual positions (Jianan v6 targets vs live positions).

   - `scheduled_processes/emails/daily/table_staleness_daily.py`  
     Daily data freshness summary for key `maicro_*` tables.

See `scheduled_processes/cron.md` for the exact crontab lines.

To install these into your user crontab, you can run:

```bash
python3 scripts/register_cron.py
```

This keeps existing unrelated cron jobs and only updates the
`maicro_monitors` entries.

## Manual / experimental jobs

These scripts exist but are not yet wired into the default cron schedule.
Run them manually while iterating on their behavior.

- Tracking error (experimental):
  - `python3 05_tracking_error/tracking_error_calculator.py --lookback 60`

- PnL calculator (experimental):
  - `python3 05_pnl_calculator/pnl_calculator.py`

- Alerts / system health (experimental):
  - `python3 ops/check_alerts.py`

Alerts use Resend; configure `RESEND_API_KEY` and email settings via
environment variables or `config/settings.py`.

## Deprecated orchestration path

Older “all‑in‑one” orchestrator scripts are still present but no longer
recommended:

- `scripts/orchestrate_monitors.py` — older ingest (Hyperliquid → buffer)
- `scripts/sync_to_remote.py` — older local → cloud sync
- `scripts/run_monitors_and_sync.sh` — previous cron wrapper

New setups should prefer the explicit `scheduled_processes/*` jobs
documented above.
