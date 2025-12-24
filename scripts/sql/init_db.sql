CREATE DATABASE IF NOT EXISTS maicro_monitors;

-- Trades (Fills)
CREATE TABLE IF NOT EXISTS maicro_monitors.trades (
    coin String,
    side String,
    px Float64,
    sz Float64,
    time DateTime64(3),
    hash String,
    startPosition Float64,
    dir String,
    closedPnl Float64,
    oid Int64,
    cloid String,
    fee Float64,
    tid Int64
) ENGINE = ReplacingMergeTree()
ORDER BY (coin, time, tid);

-- Orders (Snapshots)
CREATE TABLE IF NOT EXISTS maicro_monitors.orders (
    coin String,
    side String,
    limitPx Float64,
    sz Float64,
    oid Int64,
    timestamp DateTime64(3),
    status String, -- 'open', 'filled', 'canceled'
    orderType String,
    reduceOnly Bool
) ENGINE = ReplacingMergeTree()
ORDER BY (coin, timestamp, oid);

-- Account Snapshots
CREATE TABLE IF NOT EXISTS maicro_monitors.account_snapshots (
    timestamp DateTime64(3),
    accountValue Float64,
    totalMarginUsed Float64,
    totalNtlPos Float64,
    totalRawUsd Float64,
    marginUsed Float64,
    withdrawable Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

-- Positions Snapshots
CREATE TABLE IF NOT EXISTS maicro_monitors.positions_snapshots (
    timestamp DateTime64(3),
    coin String,
    szi Float64, -- Size
    entryPx Float64,
    positionValue Float64,
    unrealizedPnl Float64,
    returnOnEquity Float64,
    liquidationPx Float64,
    leverage Float64,
    maxLeverage Int32,
    marginUsed Float64
) ENGINE = MergeTree()
ORDER BY (coin, timestamp);

-- Funding History
CREATE TABLE IF NOT EXISTS maicro_monitors.funding_payments (
    time DateTime64(3),
    coin String,
    usdc Float64,
    szi Float64,
    fundingRate Float64,
    tid Int64 -- Using timestamp as ID if no unique ID provided, or composite
) ENGINE = ReplacingMergeTree()
ORDER BY (coin, time);

-- Candles (OHLCV)
CREATE TABLE IF NOT EXISTS maicro_monitors.candles (
    coin String,
    interval String,
    ts DateTime64(3),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (coin, interval, ts);

-- Tracking Error
CREATE TABLE IF NOT EXISTS maicro_monitors.tracking_error (
    date Date,
    strategy_id String,
    te_daily Float64,
    te_rolling_7d Float64,
    target_weight_diff Float64,
    execution_slippage Float64,
    timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (date, strategy_id);

-- Ledger Updates (Deposits/Withdrawals)
CREATE TABLE IF NOT EXISTS maicro_monitors.ledger_updates (
    time DateTime64(3),
    hash String,
    type String, -- 'deposit', 'withdraw', 'transfer', etc.
    usdc Float64, -- Amount
    coin String, -- Optional, usually USDC but maybe spot assets
    raw_json String -- Store full delta for debugging
) ENGINE = ReplacingMergeTree()
ORDER BY (time, hash);

-- Multi-lag Tracking Error
CREATE TABLE IF NOT EXISTS maicro_monitors.tracking_error_multilag (
    date Date,
    strategy_id String,
    lag Int8, -- 0, 1, 2, 3
    te Float64, -- Tracking Error (Sum of Abs Diff)
    target_date Date, -- The signal date used
    timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (date, strategy_id, lag);

-- Positions Comparison: Model vs Actual
CREATE TABLE IF NOT EXISTS maicro_monitors.positions_comparison (
    date Date,
    coin String,
    model_weight Float64,      -- Target weight from model
    model_position Float64,     -- Target position size (units)
    actual_weight Float64,      -- Actual weight achieved
    actual_position Float64,    -- Actual position size (units)
    diff_weight Float64,        -- Difference (actual - model)
    diff_position Float64,      -- Difference in position size
    model_timestamp DateTime64(3),
    actual_timestamp DateTime64(3),
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (date, coin);

-- Schema for hl_meta table
-- This table stores Hyperliquid instrument metadata including precision information

CREATE TABLE IF NOT EXISTS maicro_monitors.hl_meta (
    symbol String,
    sz_decimals Int32,
    px_decimals Int32,
    size_step Float64,
    tick_size Float64,
    min_units Float64,
    min_usd Float64,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY (symbol, updated_at);