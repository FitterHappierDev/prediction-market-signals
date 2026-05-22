"""Cross-correlation test: one PM market vs one traditional asset.

Stripped-down precursor to the Phase 3 linkage validator (REQ-LNK-001
Layer 1). Computes Pearson correlation between two time series at a
range of lags, finds the peak, and reports significance vs the
correlations at random lags.

Usage:
    python -m scripts.run_linkage_xcorr \\
        --market_id 'kalshi:KXFEDDECISION-26JUN-T3.875' \\
        --asset_ticker TLT \\
        --resample MIN \\
        --max_lag_minutes 1440

For daily-frequency analysis (the default given daily-only pm_assets):
    --resample D --max_lag_minutes 0    # contemporaneous correlation only
    --resample D --max_lag_minutes 1440 # up to ±1 day in minutes-equivalent

Outputs:
- Time series sample sizes after alignment
- Pearson r at lag 0
- Peak |r| over the scanned lag range + corresponding lag
- p-value at the peak (uncorrected and Bonferroni-corrected)
- Top 5 lags by |r| for inspection
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import warnings

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.db import QuestDBClient  # noqa: E402


def fetch_pm_series(db: QuestDBClient, market_id: str) -> pd.DataFrame:
    """Returns DataFrame indexed on UTC timestamp with column
    `probability`. One row per 1-minute bar we have for this market."""
    sql = (
        "SELECT timestamp, probability FROM pm_probabilities "
        f"WHERE market_id = '{market_id}' "
        "ORDER BY timestamp"
    )
    df = db.query(sql)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp").sort_index()
    return df


def fetch_asset_series(db: QuestDBClient, asset_ticker: str) -> pd.DataFrame:
    """Returns DataFrame indexed on UTC timestamp with column `close`.
    Pulls from pm_assets (daily). For intraday, use --intraday."""
    sql = (
        "SELECT timestamp, close FROM pm_assets "
        f"WHERE asset_ticker = '{asset_ticker}' "
        "ORDER BY timestamp"
    )
    df = db.query(sql)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp").sort_index()
    return df


def fetch_asset_intraday(asset_ticker: str, period: str, interval: str) -> pd.DataFrame:
    """On-the-fly yfinance pull bypassing pm_assets — useful for the
    minute/hour resolution needed against PM probability bars.
    `period` like '7d', `interval` like '1m', '5m', '15m', '1h'."""
    import yfinance as yf
    df = yf.download(
        asset_ticker, period=period, interval=interval,
        progress=False, auto_adjust=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index, utc=True)
    out = df[["Close"]].rename(columns={"Close": "close"})
    return out


def resample_pair(
    pm: pd.DataFrame,
    asset: pd.DataFrame,
    rule: str,
) -> pd.DataFrame:
    """Resample both series to the same regular grid, forward-filling
    the asset (markets are closed overnight / on weekends) and
    last-observation-carried-forward on the PM probability. Returns a
    merged DataFrame with columns `probability` and `close`."""
    if rule.upper() == "D":
        pm_r = pm["probability"].resample("1D").last()
        a_r = asset["close"].resample("1D").last()
    elif rule.upper() == "H":
        pm_r = pm["probability"].resample("1h").last()
        a_r = asset["close"].resample("1h").last()
    elif rule.upper() == "MIN":
        pm_r = pm["probability"].resample("1min").last()
        a_r = asset["close"].resample("1min").last()
    else:
        raise ValueError(f"unsupported resample rule: {rule}")
    df = pd.concat([pm_r, a_r], axis=1, keys=["probability", "close"]).dropna(how="all")
    df["probability"] = df["probability"].ffill()
    df["close"] = df["close"].ffill()
    return df.dropna()


def granger_test(
    a: pd.Series, b: pd.Series, max_lag: int
) -> dict[str, tuple[int, float, float]]:
    """Runs Granger causality tests in both directions, on the
    first-differenced (stationary) series.

    Returns {direction_label: (best_lag, F_stat, p_value)} for each of:
      - 'a_causes_b': does past a help predict b?
      - 'b_causes_a': does past b help predict a?

    Picks the lag with the lowest p-value within [1, max_lag].
    """
    from statsmodels.tsa.stattools import grangercausalitytests

    # First-difference for stationarity. Granger assumes stationary inputs.
    a_d = a.diff().dropna()
    b_d = b.diff().dropna()
    n = min(len(a_d), len(b_d))
    a_d = a_d.iloc[:n].values
    b_d = b_d.iloc[:n].values

    # Stack so column 0 is the predicted, column 1 is the candidate
    # cause. grangercausalitytests tests whether col 1 G-causes col 0.
    def _best(arr: np.ndarray) -> tuple[int, float, float]:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            results = grangercausalitytests(arr, maxlag=max_lag, verbose=False)
        best_lag, best_f, best_p = 0, 0.0, 1.0
        for lag, (tests, _) in results.items():
            # tests is a dict; 'ssr_ftest' returns (F, p, df_denom, df_num).
            f_val, p_val, _, _ = tests["ssr_ftest"]
            if p_val < best_p:
                best_lag = lag
                best_f = float(f_val)
                best_p = float(p_val)
        return best_lag, best_f, best_p

    # b_causes_a: data has column 0 = a (predicted), column 1 = b (cause)
    data_bca = np.column_stack([a_d, b_d])
    b_lag, b_f, b_p = _best(data_bca)

    # a_causes_b: data has column 0 = b (predicted), column 1 = a (cause)
    data_acb = np.column_stack([b_d, a_d])
    a_lag, a_f, a_p = _best(data_acb)

    return {
        "a_causes_b": (a_lag, a_f, a_p),
        "b_causes_a": (b_lag, b_f, b_p),
    }


def xcorr_scan(
    a: pd.Series,
    b: pd.Series,
    max_lag: int,
) -> tuple[list[tuple[int, float, int]], int, float, float]:
    """Compute Pearson r at lags from -max_lag to +max_lag (inclusive).
    Positive lag = a leads b (b shifted left). Returns:
    - list of (lag, r, n_overlap)
    - peak_lag
    - peak_r
    - peak_pvalue (uncorrected)
    """
    rows: list[tuple[int, float, int]] = []
    best_lag = 0
    best_r = 0.0
    best_p = 1.0
    for lag in range(-max_lag, max_lag + 1):
        if lag >= 0:
            a_aligned = a.iloc[lag:].reset_index(drop=True)
            b_aligned = b.iloc[: len(b) - lag].reset_index(drop=True)
        else:
            k = -lag
            a_aligned = a.iloc[: len(a) - k].reset_index(drop=True)
            b_aligned = b.iloc[k:].reset_index(drop=True)
        n = min(len(a_aligned), len(b_aligned))
        if n < 5:
            continue
        a_aligned = a_aligned.iloc[:n]
        b_aligned = b_aligned.iloc[:n]
        if a_aligned.std() < 1e-12 or b_aligned.std() < 1e-12:
            continue
        r, p = stats.pearsonr(a_aligned, b_aligned)
        rows.append((lag, float(r), n))
        if abs(r) > abs(best_r):
            best_r = float(r)
            best_lag = lag
            best_p = float(p)
    return rows, best_lag, best_r, best_p


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market_id", required=True, help="kalshi:TICKER or polymarket condition id")
    ap.add_argument("--asset_ticker", required=True, help="e.g. TLT, EURUSD=X, BTC-USD")
    ap.add_argument("--resample", default="D", choices=["D", "H", "MIN"], help="resample frequency")
    ap.add_argument("--max_lag", type=int, default=5, help="max lag in resample-period units")
    ap.add_argument("--intraday", action="store_true",
                    help="bypass pm_assets and pull yfinance directly at high frequency")
    ap.add_argument("--intraday_period", default="7d", help="yfinance period when --intraday")
    ap.add_argument("--intraday_interval", default="1h", help="yfinance interval when --intraday")
    ap.add_argument("--granger", action="store_true",
                    help="also run Layer 2 Granger causality test (both directions)")
    ap.add_argument("--granger_max_lag", type=int, default=5,
                    help="max lag for Granger (in resample-period units)")
    args = ap.parse_args()

    db = QuestDBClient()

    pm = fetch_pm_series(db, args.market_id)
    if args.intraday:
        a = fetch_asset_intraday(args.asset_ticker, args.intraday_period, args.intraday_interval)
    else:
        a = fetch_asset_series(db, args.asset_ticker)

    print(f"PM market {args.market_id}: {len(pm)} bars  "
          f"({pm.index.min() if not pm.empty else '-'} → {pm.index.max() if not pm.empty else '-'})")
    print(f"Asset  {args.asset_ticker}: {len(a)} bars  "
          f"({a.index.min() if not a.empty else '-'} → {a.index.max() if not a.empty else '-'})")
    if pm.empty or a.empty:
        print("\nERROR: one or both series is empty", file=sys.stderr)
        return 2

    merged = resample_pair(pm, a, args.resample)
    print(f"\nAfter resampling to {args.resample} and aligning: {len(merged)} overlapping rows")
    print(f"  prob  range: {merged['probability'].min():.4f} → {merged['probability'].max():.4f}  "
          f"std={merged['probability'].std():.4f}")
    print(f"  close range: {merged['close'].min():.4f} → {merged['close'].max():.4f}  "
          f"std={merged['close'].std():.4f}")

    if len(merged) < 5:
        print("\nERROR: <5 overlapping rows; cannot compute reliable correlation", file=sys.stderr)
        return 3

    rows, best_lag, best_r, best_p = xcorr_scan(
        merged["probability"], merged["close"], args.max_lag
    )
    n_tested = len(rows)
    bonf_p = min(1.0, best_p * n_tested) if n_tested > 0 else 1.0

    print(f"\nCross-correlation scan: {n_tested} lags tested in [-{args.max_lag}, +{args.max_lag}]")
    print(f"Peak  lag = {best_lag:+d} {args.resample}")
    print(f"Peak  r   = {best_r:+.4f}")
    print(f"  p-value (uncorrected) = {best_p:.4g}")
    print(f"  p-value (Bonferroni)  = {bonf_p:.4g}")

    print(f"\nTop 5 lags by |r|:")
    rows_sorted = sorted(rows, key=lambda x: abs(x[1]), reverse=True)
    for lag, r, n in rows_sorted[:5]:
        print(f"  lag {lag:+4d}  r = {r:+.4f}   n = {n}")

    if args.granger:
        print(f"\n--- Layer 2: Granger causality (max_lag={args.granger_max_lag}) ---")
        try:
            results = granger_test(
                merged["probability"], merged["close"], args.granger_max_lag
            )
        except Exception as e:
            print(f"Granger failed: {e}", file=sys.stderr)
            return 4

        pm_to_asset = results["a_causes_b"]
        asset_to_pm = results["b_causes_a"]
        print(f"PM probability  →  asset close:   best lag={pm_to_asset[0]}  "
              f"F={pm_to_asset[1]:.2f}  p={pm_to_asset[2]:.4g}")
        print(f"asset close    →  PM probability: best lag={asset_to_pm[0]}  "
              f"F={asset_to_pm[1]:.2f}  p={asset_to_pm[2]:.4g}")

        threshold = 0.05
        pm_leads = pm_to_asset[2] < threshold
        asset_leads = asset_to_pm[2] < threshold
        print(f"\nInterpretation (p<{threshold}):")
        if pm_leads and not asset_leads:
            print("  PM market UNIDIRECTIONALLY Granger-causes asset (potential alpha)")
        elif asset_leads and not pm_leads:
            print("  Asset Granger-causes PM (market reflects asset, not vice versa)")
        elif pm_leads and asset_leads:
            print("  Bidirectional Granger causality (likely contemporaneous mechanism)")
        else:
            print("  Neither direction passes — just correlation, no Granger causation")

    return 0


if __name__ == "__main__":
    sys.exit(main())
