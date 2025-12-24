# Scheduled Processes: Cron Inventory

This file documents the intended cron entries and cadences for the
`scheduled_processes/` jobs. Paths use `$REPO_ROOT` as the project root
for the `maicro_monitors` repo.

## 1. Hyperliquid ingest (buffer + flush)

### 1.1 Hourly buffer-only ping

**Script:** `scheduled_processes/scheduled_ping_hyperliquid.py`  
**Purpose:** Call Hyperliquid HTTP APIs and write results to
`data/buffer/*.parquet` via `BufferManager.save()`:

- `account`   → `account_snapshots` buffer
- `positions` → `positions_snapshots` buffer
- `trades`    → `trades` buffer
- `orders`    → `orders` buffer
- `funding`   → `funding_payments` buffer
- `ledger`    → `ledger_updates` buffer
- `candles`   → `candles` buffer

This script does **not** talk to ClickHouse.

**Cadence:**

```cron
*/15 * * * * cd $REPO_ROOT && /usr/bin/python3 scheduled_processes/scheduled_ping_hyperliquid.py >> logs/hyperliquid_ping.log 2>&1
```

### 1.2 3‑hour dual-target buffer flush

**Script:** `scheduled_processes/flush_hyperliquid_buffers.py`  
**Purpose:** Read all buffered Parquet files from `data/buffer/` and
insert into both:

1. Local / chenlin host (`CLICKHOUSE_LOCAL_CONFIG`)
2. ClickHouse Cloud (`CLICKHOUSE_REMOTE_CONFIG`)

Also runs `OPTIMIZE TABLE {table} FINAL` on `ReplacingMergeTree` tables
to deduplicate records.

Targets:

- `account`   → `maicro_monitors.account_snapshots`
- `positions` → `maicro_monitors.positions_snapshots`
- `trades`    → `maicro_monitors.trades`
- `orders`    → `maicro_monitors.orders`
- `funding`   → `maicro_monitors.funding_payments`
- `ledger`    → `maicro_monitors.ledger_updates`
- `candles`   → `maicro_monitors.candles`

**Cadence (ClickHouse-friendly):**

```cron
0 */3 * * * cd $REPO_ROOT && /usr/bin/python3 scheduled_processes/flush_hyperliquid_buffers.py >> logs/hyperliquid_flush.log 2>&1
```

## 2. Daily Emails

Two separate daily operational emails:

1. **Target vs Actual Positions**  
   **Script:** `scheduled_processes/emails/daily/targets_vs_actuals_daily.py`  
   **Purpose:** Compare latest strategy targets (`maicro_logs.positions_jianan_v6`)
   vs actual positions (`maicro_monitors.positions_snapshots`) and highlight
   per-coin deltas.

2. **Maicro Tables Staleness Checker**  
   **Script:** `scheduled_processes/emails/daily/table_staleness_daily.py`  
   **Purpose:** Produce a daily table of key `maicro_*` tables with:
   table name, last timestamp, age (e.g. "7m ago"), and status (OK / stale).

**Cadence (once per day, 08:00 UTC):**
```cron
0 8 * * * cd $REPO_ROOT && /usr/bin/python3 scheduled_processes/emails/daily/targets_vs_actuals_daily.py >> logs/email_targets_vs_actuals.log 2>&1
5 8 * * * cd $REPO_ROOT && /usr/bin/python3 scheduled_processes/emails/daily/table_staleness_daily.py >> logs/email_table_staleness.log 2>&1
10 8 * * * cd $REPO_ROOT && /usr/bin/python3 scheduled_processes/emails/daily/trades_last24h_daily.py >> logs/email_trades_last24h.log 2>&1
15 8 * * * cd $REPO_ROOT && /usr/bin/python3 scheduled_processes/emails/daily/tracking_error_last3d_daily.py >> logs/email_tracking_error_last3d.log 2>&1
```

> Legacy: `reports/daily_summary_email.py` is still available as a
> ClickHouse-based daily summary if you want a single combined email; if
> you cron it, document that separately from this file.

## 3. Cloud → chenlin down-sync

**Script:** `scheduled_processes/pull_data_downward_from_cloud.py`  
**Purpose:** Pull data from ClickHouse Cloud down to
`chenlin04.fbe.hku.hk` (local host) for the databases:

- `hyperliquid`
- `maicro_logs`
- `binance`

Behavior:

- For each table:
  - If missing locally: `SHOW CREATE TABLE` from cloud, normalize engine,
    create locally, initial full copy via `remoteSecure`.
  - If present: incremental sync using a date/timestamp cursor column
    (override or inferred) so only new rows are pulled.

**Cadence (every 6 hours):**

```cron
0 */6 * * * cd $REPO_ROOT && /usr/bin/python3 scheduled_processes/pull_data_downward_from_cloud.py >> logs/pull_from_cloud.log 2>&1
```
