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


def analyze_pnl_returns_daily(df, nav_col='totalNtlPos', ts_col='ts'):
    """
    Calculate daily returns from NAV data.
    
    Returns:
        tuple: (metrics_dict, result_df)
    """
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col])
    df = df.sort_values(ts_col).set_index(ts_col)
    
    # Resample to daily frequency
    nav_daily = df[nav_col].resample('1D').last().dropna()
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


def load_live_data(lookback_days=30):
    """Load live trading data from maicro_logs.live_account"""
    query = f"""
    SELECT ts, totalNtlPos, accountValue, time
    FROM maicro_logs.live_account
    WHERE ts >= now() - INTERVAL {lookback_days} DAY
    ORDER BY ts
    """
    df = query_df(query)
    return df


def load_paper_data(lookback_days=30):
    """
    Load paper portfolio data (target weights + market returns).
    This needs to be adapted based on where your target weights are stored.
    """
    # TODO: Replace with actual query for your target weights
    # Example structure:
    # query = f"""
    # SELECT date, symbol, target_weight
    # FROM your_database.target_weights
    # WHERE date >= now() - INTERVAL {lookback_days} DAY
    # """
    # weights_df = query_df(query)
    
    # For now, return None to indicate this needs implementation
    print("⚠️  Paper portfolio data loading not yet implemented")
    print("    Need to specify where target weights are stored")
    return None


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
    
    # 3. Load paper portfolio data
    print("\n3. Loading paper portfolio (target weights + returns)...")
    paper_data = load_paper_data(lookback_days)
    
    if paper_data is None:
        print("⚠️  Skipping tracking error calculation (paper data not available)")
        print("\nTo enable full tracking error calculation:")
        print("  1. Store target weights in ClickHouse")
        print("  2. Update load_paper_data() function with proper query")
        print("  3. Ensure market returns data is available")
        return
    
    # 4. Calculate paper portfolio returns
    # paper_returns = calculate_strategy_returns(market_returns, target_weights)
    
    # 5. Calculate tracking error
    # te_metrics = calculate_tracking_error(live_daily['return'], paper_returns)
    
    # 6. Store results
    # (Implementation pending)
    
    print("\n" + "=" * 60)
    print("Calculation complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
