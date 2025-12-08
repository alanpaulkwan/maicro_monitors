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
