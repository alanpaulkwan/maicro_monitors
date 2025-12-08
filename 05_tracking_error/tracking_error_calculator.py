#!/usr/bin/env python3
"""
Tracking Error Calculator - Based on test_pnl_analysis.ipynb methodology
Compares actual live trading performance vs target paper portfolio performance.

Methodology:
1. Load live trading NAV data from maicro_logs.live_account
2. Calculate paper portfolio returns from target_weights × market_returns  
3. Compute tracking error as the difference in returns/performance
4. Store results in maicro_monitors.tracking_error table
"""
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.clickhouse_client import query_df


def analyze_pnl_returns_daily(df, nav_value_col='equity_usd', timestamp_col='ts'):
    """
    Calculate daily returns from NAV data.
    
    Returns:
        tuple: (metrics_dict, result_df)
    """
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    df = df.sort_values(timestamp_col).set_index(timestamp_col)
    
    # Resample to daily frequency
    nav_daily = df[nav_value_col].resample('1D').last().dropna()
    returns = nav_daily.pct_change().dropna()
    
    if len(returns) < 2:
        return None, pd.DataFrame({'nav': nav_daily, 'return': returns})
    
    # Annualization factors (252 trading days)
    ann_factor = np.sqrt(252)
    avg_daily = returns.mean()
    std_daily = returns.std()
    ann_return = (1 + avg_daily) ** 252 - 1
    ann_vol = std_daily * ann_factor
    sharpe = (avg_daily / std_daily) * ann_factor if std_daily != 0 else np.nan
    
    metrics = {
        'sharpe': sharpe,
        'annualized_return': ann_return,
        'annualized_volatility': ann_vol,
        'avg_daily_return': avg_daily,
        'N_daily_returns': len(returns)
    }
    
    result_df = pd.DataFrame({'nav': nav_daily, 'return': returns})
    return metrics, result_df


def load_live_data(lookback_days=30):
    """Load live trading data from maicro_logs.live_account"""
    query = f"""
    SELECT ts, equity_usd
    FROM maicro_logs.live_account
    WHERE ts >= now() - INTERVAL {lookback_days} DAY
    ORDER BY ts
    """
    df = query_df(query)
    return df


def load_paper_data(lookback_days=30):
    """
    Load paper portfolio data (target weights) from maicro_logs.positions_jianan_v6.
    
    Returns:
        pd.DataFrame: DataFrame with date, symbol, and weight for target positions.
    """
    query = f"""
    SELECT
        date,
        symbol,
        weight
    FROM maicro_logs.positions_jianan_v6
    WHERE (date, symbol, inserted_at) IN (
        SELECT
            date,
            symbol,
            max(inserted_at) as max_inserted_at
        FROM maicro_logs.positions_jianan_v6
        WHERE date >= toDate(now() - INTERVAL {lookback_days} DAY)
        GROUP BY
            date,
            symbol
    )
    AND weight IS NOT NULL
    ORDER BY date, symbol
    """
    df = query_df(query)
    df['date'] = pd.to_datetime(df['date']) # Ensure date is datetime
    df['symbol'] = df['symbol'].str.upper() # Standardize symbols to uppercase
    return df

def calculate_strategy_returns(returns_df, weights_df, shift_period=1):
    """
    Calculate paper portfolio returns from target weights and market returns.
    
    Parameters:
        returns_df: Market returns (date x symbol)
        weights_df: Target weights (date x symbol)
        shift_period: Lag for weights (1 = use previous day's weights)
    
    Returns:
        Series: Daily strategy returns
    """
    aligned_returns = returns_df.reindex(
        index=weights_df.index,
        columns=weights_df.columns
    )
    
    # Strategy returns = sum(returns * lagged_weights)
    strategy_returns = (aligned_returns * weights_df.shift(shift_period)).sum(1)
    return strategy_returns

def calculate_tracking_error(live_returns, paper_returns):
    """
    Calculate tracking error between live and paper portfolio.
    
    Returns:
        dict: Tracking error metrics
    """
    # Align dates
    common_dates = live_returns.index.intersection(paper_returns.index)
    live_aligned = live_returns.loc[common_dates]
    paper_aligned = paper_returns.loc[common_dates]
    
    # Calculate tracking difference
    tracking_diff = live_aligned - paper_aligned
    
    # Metrics
    te_daily_mean = tracking_diff.mean()
    te_daily_std = tracking_diff.std()
    te_annualized = te_daily_std * np.sqrt(252)
    
    # Cumulative tracking error
    cum_te = (1 + tracking_diff).cumprod() - 1
    
    return {
        'te_daily_mean': te_daily_mean,
        'te_daily_std': te_daily_std, 
        'te_annualized': te_annualized,
        'tracking_diff': tracking_diff,
        'cum_tracking_error': cum_te
    }


def load_market_returns(lookback_days=30):
    """
    Load market close prices from maicro_monitors.candles and calculate daily returns.
    
    Returns:
        pd.DataFrame: DataFrame with daily returns, indexed by date, columns as symbols.
    """
    query = f"""
    SELECT toDate(ts) as date, coin, close
    FROM maicro_monitors.candles
    WHERE ts >= toStartOfHour(now() - INTERVAL {lookback_days + 5} DAY)  -- Add buffer for return calculation
    AND interval = '1d'
    ORDER BY date, coin
    """
    df = query_df(query)
    
    if df.empty:
        return pd.DataFrame()
    
    df['date'] = pd.to_datetime(df['date'])
    df['coin'] = df['coin'].str.upper() # Standardize coins to uppercase
    
    # Pivot to get coins as columns and date as index
    prices_pivot = df.pivot_table(index='date', columns='coin', values='close')
    
    # Calculate daily returns
    returns = prices_pivot.pct_change()
    
    return returns.dropna(how='all') # Drop rows where all returns are NaN

def store_tracking_error_results(te_metrics, strategy_id="default_strategy"):
    """
    Stores tracking error results into maicro_monitors.tracking_error table.
    """
    from modules.clickhouse_client import insert_df
    
    # Process tracking_diff (Series) into a DataFrame to get daily values
    tracking_diff_df = pd.DataFrame({
        'date': te_metrics['tracking_diff'].index,
        'te_daily': te_metrics['tracking_diff'].values
    })
    
    # Calculate rolling 7-day TE (example: rolling mean of daily tracking difference)
    # This might need to be calculated on the 'te_daily' after it's been stored and retrieved
    # For simplicity, calculate a rolling mean of the daily tracking difference for now.
    tracking_diff_df['te_rolling_7d'] = tracking_diff_df['te_daily'].rolling(window=7).mean()
    
    # For now, map te_annualized to each daily entry or store as a single aggregate entry.
    # The schema implies daily records.
    # Let's create a record for each date where we have tracking difference.
    
    records = []
    for index, row in tracking_diff_df.iterrows():
        records.append({
            'date': row['date'].date(), # Convert to date object
            'strategy_id': strategy_id,
            'te_daily': row['te_daily'] if not pd.isna(row['te_daily']) else 0.0, # Handle NaN
            'te_rolling_7d': row['te_rolling_7d'] if not pd.isna(row['te_rolling_7d']) else 0.0,
            'target_weight_diff': 0.0, # Placeholder
            'execution_slippage': 0.0, # Placeholder
            'timestamp': datetime.now()
        })
    
    if records:
        df_to_store = pd.DataFrame(records)
        insert_df('maicro_monitors.tracking_error', df_to_store)
        print(f"   ✓ Stored {len(df_to_store)} tracking error records.")
    else:
        print("   No tracking error records to store.")

def main():
    """Main tracking error calculation pipeline"""
    print("=" * 60)
    print("Tracking Error Calculator")
    print("=" * 60)
    
    lookback_days = 30
    
    # 1. Load live trading data
    print(f"\n1. Loading live trading data (last {lookback_days} days)...")
    live_df = load_live_data(lookback_days)
    
    if live_df.empty:
        print("❌ No live trading data found")
        return
    
    print(f"   ✓ Loaded {len(live_df)} records")
    
    # 2. Calculate live portfolio returns
    print("\n2. Calculating live portfolio returns...")
    live_metrics, live_daily = analyze_pnl_returns_daily(live_df)
    
    if live_metrics is None:
        print("❌ Insufficient data for return calculation")
        return
    
    print(f"   ✓ Live Sharpe: {live_metrics['sharpe']:.3f}")
    print(f"   ✓ Live Ann. Return: {live_metrics['annualized_return']*100:.2f}%")
    print(f"   ✓ Live Ann. Vol: {live_metrics['annualized_volatility']*100:.2f}%")
    
    # 3. Load paper portfolio target weights
    print("\n3. Loading paper portfolio target weights...")
    target_weights_df = load_paper_data(lookback_days)
    
    if target_weights_df.empty:
        print("⚠️  No paper portfolio target weights found. Skipping further calculation.")
        return
    
    print(f"   ✓ Loaded {len(target_weights_df)} target weight records.")

    # Convert target_weights_df to a pivot table suitable for calculate_strategy_returns
    weights_pivot = target_weights_df.pivot(index='date', columns='symbol', values='weight')
    
    print(f"   - Target weights date range: {weights_pivot.index.min()} to {weights_pivot.index.max()}")
    print(f"   - Target weights unique symbols ({len(weights_pivot.columns)}): {weights_pivot.columns.tolist()[:5]}...") # Print first 5
    
    # 4. Loading market returns data
    print("\n4. Loading market returns data...")
    market_returns_df = load_market_returns(lookback_days)
    
    if market_returns_df.empty:
        print("❌ No market returns data found. Skipping further calculation.")
        return
    print(f"   ✓ Loaded market returns for {len(market_returns_df.columns)} coins and {len(market_returns_df)} days.")
    print(f"   - Market returns date range: {market_returns_df.index.min()} to {market_returns_df.index.max()}")
    print(f"   - Market returns unique symbols ({len(market_returns_df.columns)}): {market_returns_df.columns.tolist()[:5]}...") # Print first 5
    
    # Align market returns and weights to common dates and symbols
    common_index = market_returns_df.index.intersection(weights_pivot.index)
    common_columns = market_returns_df.columns.intersection(weights_pivot.columns)

    print(f"\n   - Common dates: {len(common_index)} (from {common_index.min()} to {common_index.max()})")
    print(f"   - Common symbols: {len(common_columns)}")
    
    market_returns_aligned = market_returns_df.loc[common_index, common_columns]
    weights_pivot_aligned = weights_pivot.loc[common_index, common_columns]

    if market_returns_aligned.empty or weights_pivot_aligned.empty:
        print("❌ No common dates or symbols between market returns and target weights. Skipping further calculation.")
        return
    
    print("\n5. Calculating paper portfolio returns...")
    paper_returns = calculate_strategy_returns(market_returns_aligned, weights_pivot_aligned)
    
    if paper_returns.empty:
        print("❌ Could not calculate paper portfolio returns.")
        return

    # 6. Calculate tracking error
    print("\n6. Calculating tracking error...")
    te_metrics = calculate_tracking_error(live_daily['return'], paper_returns)
    
    print("\n   ✓ Tracking Error (Annualized Std Dev):", te_metrics['te_annualized'])
    print("   ✓ Tracking Difference (Mean):", te_metrics['te_daily_mean'])

    # 7. Store results
    # (Implementation pending - store te_metrics or a time series of tracking_diff)
    print("\n7. Storing tracking error results...")
    store_tracking_error_results(te_metrics)
    
    print("\n" + "=" * 60)
    print("Calculation complete")
    print("=" * 60)


if __name__ == "__main__":
    main()

