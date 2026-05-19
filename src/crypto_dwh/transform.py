import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .config import Config

logger = logging.getLogger(__name__)

_ASSET_META: dict[str, dict] = {
    "BTC-USD": {"name": "Bitcoin", "category": "Cryptocurrency"},
    "ETH-USD": {"name": "Ethereum", "category": "Cryptocurrency"},
    "BNB-USD": {"name": "BNB", "category": "Cryptocurrency"},
    "SOL-USD": {"name": "Solana", "category": "Cryptocurrency"},
    "ADA-USD": {"name": "Cardano", "category": "Cryptocurrency"},
}

_SENTIMENT_BINS: list[dict] = [
    {"sentiment_key": 1, "fng_classification": "extreme_fear", "fng_label": "Extreme Fear", "fng_min": 0, "fng_max": 24},
    {"sentiment_key": 2, "fng_classification": "fear", "fng_label": "Fear", "fng_min": 25, "fng_max": 49},
    {"sentiment_key": 3, "fng_classification": "neutral", "fng_label": "Neutral", "fng_min": 50, "fng_max": 54},
    {"sentiment_key": 4, "fng_classification": "greed", "fng_label": "Greed", "fng_min": 55, "fng_max": 74},
    {"sentiment_key": 5, "fng_classification": "extreme_greed", "fng_label": "Extreme Greed", "fng_min": 75, "fng_max": 100},
]


def _load_latest(prefix: str) -> pd.DataFrame:
    files = sorted(Config.PROCESSED_DIR.glob(f"{prefix}_*.csv"))
    if not files:
        raise FileNotFoundError(f"no processed file found for prefix '{prefix}' in {Config.PROCESSED_DIR}")
    path = files[-1]
    logger.info("loading %s", path.name)
    return pd.read_csv(path)


def _compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _fng_classification(val: int) -> str:
    if val <= 24:
        return "extreme_fear"
    if val <= 49:
        return "fear"
    if val <= 54:
        return "neutral"
    if val <= 74:
        return "greed"
    return "extreme_greed"


def _aggregate_to_daily(df_hourly: pd.DataFrame) -> pd.DataFrame:
    df_hourly = df_hourly.copy()
    df_hourly["datetime_utc"] = pd.to_datetime(df_hourly["datetime_utc"], utc=True)
    df_hourly.sort_values(["symbol", "datetime_utc"], inplace=True)

    agg = (
        df_hourly.groupby(["symbol", "date"])
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )
        .reset_index()
    )
    logger.info("aggregated %d hourly rows -> %d daily rows", len(df_hourly), len(agg))
    return agg


def _compute_indicators(df_daily: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for symbol, grp in df_daily.groupby("symbol"):
        grp = grp.sort_values("date").copy()
        grp["daily_return"] = grp["close"].pct_change()
        grp["ma7"] = grp["close"].rolling(7, min_periods=1).mean()
        grp["ma30"] = grp["close"].rolling(30, min_periods=1).mean()
        grp["volatility_7d"] = grp["daily_return"].rolling(7, min_periods=2).std()
        grp["rsi_14"] = _compute_rsi(grp["close"])
        vol_mean = grp["volume"].rolling(30, min_periods=5).mean()
        vol_std = grp["volume"].rolling(30, min_periods=5).std()
        grp["volume_zscore"] = (grp["volume"] - vol_mean) / vol_std.replace(0, np.nan)
        grp["is_volume_anomaly"] = grp["volume_zscore"].abs() > 2
        grp["trend_label"] = np.where(
            grp["ma7"] > grp["ma30"], "bullish",
            np.where(grp["ma7"] < grp["ma30"], "bearish", "neutral")
        )
        grp["rsi_signal"] = np.where(
            grp["rsi_14"] < 30, "oversold",
            np.where(grp["rsi_14"] > 70, "overbought", "neutral")
        )
        grp["rsi_signal"] = grp["rsi_signal"].where(grp["rsi_14"].notna(), "neutral")
        frames.append(grp)
    result = pd.concat(frames, ignore_index=True)
    logger.info("computed indicators for %d daily rows", len(result))
    return result


def build_dim_time(dates: pd.Series) -> pd.DataFrame:
    unique_dates = pd.to_datetime(dates.unique())
    df = pd.DataFrame({"date": unique_dates})
    df.sort_values("date", inplace=True)
    df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    df["month"] = df["date"].dt.month
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["day_of_week"] = df["date"].dt.dayofweek
    df["day_name"] = df["date"].dt.day_name()
    df["is_weekend"] = df["day_of_week"] >= 5
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df.reset_index(drop=True, inplace=True)
    logger.info("built dim_time with %d rows", len(df))
    return df


def build_dim_asset(symbols: Optional[list[str]] = None) -> pd.DataFrame:
    symbols = symbols or Config.SYMBOLS
    rows = []
    for idx, symbol in enumerate(symbols, start=1):
        meta = _ASSET_META.get(symbol, {"name": symbol, "category": "Cryptocurrency"})
        rows.append({"asset_key": idx, "symbol": symbol, **meta})
    df = pd.DataFrame(rows)
    logger.info("built dim_asset with %d rows", len(df))
    return df


def build_dim_sentiment() -> pd.DataFrame:
    df = pd.DataFrame(_SENTIMENT_BINS)
    logger.info("built dim_sentiment with %d rows", len(df))
    return df


def build_dim_trend(df_daily: pd.DataFrame) -> pd.DataFrame:
    combos = (
        df_daily[["trend_label", "rsi_signal"]]
        .drop_duplicates()
        .sort_values(["trend_label", "rsi_signal"])
        .reset_index(drop=True)
    )
    combos["trend_key"] = combos.index + 1
    df = combos[["trend_key", "trend_label", "rsi_signal"]]
    logger.info("built dim_trend with %d rows", len(df))
    return df


def build_fact_market_hourly(df_hourly: pd.DataFrame, dim_time: pd.DataFrame, dim_asset: pd.DataFrame) -> pd.DataFrame:
    df = df_hourly.copy()
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["hour"] = df["datetime_utc"].dt.hour
    df["date"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")
    df["datetime_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df = df.merge(dim_time[["date_key", "date"]], on="date", how="left")
    df = df.merge(dim_asset[["asset_key", "symbol"]], on="symbol", how="left")
    df["fact_key"] = range(1, len(df) + 1)
    cols = ["fact_key", "date_key", "asset_key", "datetime_utc", "hour", "open", "high", "low", "close", "adj_close", "volume"]
    df = df[[c for c in cols if c in df.columns]]
    df.sort_values(["asset_key", "datetime_utc"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    logger.info("built fact_market_hourly with %d rows", len(df))
    return df


def build_fact_market_daily(
    df_daily: pd.DataFrame,
    df_fng: pd.DataFrame,
    dim_time: pd.DataFrame,
    dim_asset: pd.DataFrame,
    dim_sentiment: pd.DataFrame,
    dim_trend: pd.DataFrame,
) -> pd.DataFrame:
    df_fng = df_fng.copy()
    df_fng["fng_classification"] = df_fng["fng_value"].astype(int).apply(_fng_classification)

    df = df_daily.merge(df_fng[["date", "fng_value", "fng_classification"]], on="date", how="left")
    df = df.merge(dim_time[["date_key", "date"]], on="date", how="left")
    df = df.merge(dim_asset[["asset_key", "symbol"]], on="symbol", how="left")
    df = df.merge(dim_sentiment[["sentiment_key", "fng_classification"]], on="fng_classification", how="left")
    df = df.merge(dim_trend[["trend_key", "trend_label", "rsi_signal"]], on=["trend_label", "rsi_signal"], how="left")

    df["fact_key"] = range(1, len(df) + 1)

    cols = [
        "fact_key", "date_key", "asset_key", "sentiment_key", "trend_key",
        "open", "high", "low", "close", "volume",
        "fng_value", "daily_return", "ma7", "ma30", "volatility_7d",
        "rsi_14", "volume_zscore", "is_volume_anomaly",
    ]
    df = df[[c for c in cols if c in df.columns]]
    df.sort_values(["asset_key", "date_key"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    logger.info("built fact_market_daily with %d rows", len(df))
    return df


def _save_parquet(df: pd.DataFrame, name: str, run_ts: str) -> Path:
    path = Config.PROCESSED_DIR / f"{name}_{run_ts}.parquet"
    df.to_parquet(path, index=False)
    logger.info("saved %s (%d rows, %d cols)", path.name, len(df), len(df.columns))
    return path


def run_transform(extraction_result: Optional[dict] = None, run_ts: Optional[str] = None) -> dict:
    run_ts = run_ts or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info("transform started | run_ts=%s", run_ts)

    if extraction_result:
        df_hourly = pd.concat(extraction_result["ohlcv"].values(), ignore_index=True)
        df_fng = extraction_result["fng"]
    else:
        df_hourly = _load_latest("ohlcv_all")
        df_fng = _load_latest("fng")

    df_daily_raw = _aggregate_to_daily(df_hourly)
    df_daily = _compute_indicators(df_daily_raw)

    all_dates = pd.concat([df_daily["date"], df_fng["date"]]).drop_duplicates()

    dim_time = build_dim_time(all_dates)
    dim_asset = build_dim_asset()
    dim_sentiment = build_dim_sentiment()
    dim_trend = build_dim_trend(df_daily)

    fact_hourly = build_fact_market_hourly(df_hourly, dim_time, dim_asset)
    fact_daily = build_fact_market_daily(df_daily, df_fng, dim_time, dim_asset, dim_sentiment, dim_trend)

    results = {
        "run_ts": run_ts,
        "dim_time": dim_time,
        "dim_asset": dim_asset,
        "dim_sentiment": dim_sentiment,
        "dim_trend": dim_trend,
        "fact_market_hourly": fact_hourly,
        "fact_market_daily": fact_daily,
    }

    for name, df in results.items():
        if isinstance(df, pd.DataFrame):
            _save_parquet(df, name, run_ts)

    logger.info("transform complete | dim_time=%d dim_asset=%d dim_sentiment=%d dim_trend=%d fact_hourly=%d fact_daily=%d",
        len(dim_time), len(dim_asset), len(dim_sentiment), len(dim_trend), len(fact_hourly), len(fact_daily))

    return results


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    result = run_transform()
    for name, obj in result.items():
        if isinstance(obj, pd.DataFrame):
            print(f"{name}: {obj.shape}")
