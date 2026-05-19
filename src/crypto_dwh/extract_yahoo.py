import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from .config import Config

logger = logging.getLogger(__name__)


def _unix_ts(dt: datetime) -> int:
    return int(dt.timestamp())


def _date_chunks(start: str, end: str, chunk_days: int) -> list[tuple[datetime, datetime]]:
    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    chunks = []
    cursor = start_dt
    while cursor < end_dt:
        chunk_end = min(cursor + timedelta(days=chunk_days), end_dt)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
    return chunks


def _build_params(symbol: str, start_dt: datetime, end_dt: datetime) -> dict:
    return {
        "symbol": symbol,
        "period1": _unix_ts(start_dt),
        "period2": _unix_ts(end_dt),
        "interval": Config.YF_INTERVAL,
        "includePrePost": "false",
        "events": "div,splits",
    }


def _get(url: str, params: dict) -> dict:
    last_exc: Optional[Exception] = None
    for attempt in range(1, Config.REQUEST_MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=Config.YF_HEADERS,
                timeout=Config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            wait = Config.REQUEST_BACKOFF_S * (2 ** (attempt - 1))
            logger.warning("attempt %d/%d failed, retrying in %.1fs", attempt, Config.REQUEST_MAX_RETRIES, wait)
            time.sleep(wait)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                wait = Config.REQUEST_BACKOFF_S * (2 ** (attempt - 1))
                logger.warning("rate limited (429), retrying in %.1fs", wait)
                time.sleep(wait)
                last_exc = exc
            else:
                raise
    raise requests.ConnectionError(f"all {Config.REQUEST_MAX_RETRIES} retries failed") from last_exc


def _parse_chunk(data: dict, symbol: str) -> pd.DataFrame:
    try:
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        quote = result["indicators"]["quote"][0]
        adjclose = result["indicators"].get("adjclose", [{}])[0]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError(f"unexpected response structure for {symbol}: {exc}") from exc

    df = pd.DataFrame(
        {
            "datetime_utc": pd.to_datetime(timestamps, unit="s", utc=True),
            "symbol": symbol,
            "open": quote.get("open"),
            "high": quote.get("high"),
            "low": quote.get("low"),
            "close": quote.get("close"),
            "adj_close": adjclose.get("adjclose") if adjclose else None,
            "volume": quote.get("volume"),
        }
    )
    df.dropna(subset=["open", "high", "low", "close", "volume"], how="all", inplace=True)
    return df


def _dump_json(data: dict, symbol: str, run_ts: str, chunk_idx: int) -> Path:
    path = Config.RAW_DIR / f"yahoo_{symbol.replace('-', '_')}_{run_ts}_chunk{chunk_idx:03d}.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    return path


def _dump_csv(df: pd.DataFrame, symbol: str, run_ts: str) -> Path:
    path = Config.RAW_DIR / f"yahoo_{symbol.replace('-', '_')}_{run_ts}.csv"
    df.to_csv(path, index=False)
    logger.info("saved %s (%d rows)", path.name, len(df))
    return path


def fetch_ohlcv(symbol: str, run_ts: Optional[str] = None, save_raw: bool = True) -> pd.DataFrame:
    """Fetch hourly OHLCV for one symbol using chunked requests to bypass the 60-day limit."""
    run_ts = run_ts or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    url = f"{Config.YF_BASE_URL}{symbol}"
    chunks = _date_chunks(Config.START_DATE, Config.END_DATE, Config.YF_CHUNK_DAYS)

    logger.info(
        "fetching %s | %s -> %s | interval=%s | %d chunks",
        symbol, Config.START_DATE, Config.END_DATE, Config.YF_INTERVAL, len(chunks),
    )

    frames: list[pd.DataFrame] = []
    for idx, (chunk_start, chunk_end) in enumerate(chunks):
        params = _build_params(symbol, chunk_start, chunk_end)
        raw = _get(url, params)
        if save_raw:
            _dump_json(raw, symbol, run_ts, idx)
        try:
            df_chunk = _parse_chunk(raw, symbol)
            frames.append(df_chunk)
            logger.debug("chunk %d/%d -> %d rows", idx + 1, len(chunks), len(df_chunk))
        except ValueError as exc:
            logger.warning("skipping chunk %d for %s: %s", idx, symbol, exc)
        time.sleep(0.5)

    if not frames:
        raise ValueError(f"no data returned for {symbol}")

    df = pd.concat(frames, ignore_index=True)
    df.drop_duplicates(subset=["datetime_utc", "symbol"], inplace=True)
    df.sort_values("datetime_utc", inplace=True)
    df.reset_index(drop=True, inplace=True)

    df["date"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")
    df["datetime_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info("done %s | %d hourly rows | %s -> %s", symbol, len(df), df["date"].iloc[0], df["date"].iloc[-1])

    if save_raw:
        _dump_csv(df, symbol, run_ts)

    return df


def fetch_all_symbols(symbols: Optional[list[str]] = None, run_ts: Optional[str] = None, save_raw: bool = True) -> dict[str, pd.DataFrame]:
    """Fetch hourly OHLCV for all configured symbols, returning a dict of DataFrames."""
    symbols = symbols or Config.SYMBOLS
    run_ts = run_ts or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    results: dict[str, pd.DataFrame] = {}
    failed: list[str] = []
    for symbol in symbols:
        try:
            results[symbol] = fetch_ohlcv(symbol, run_ts=run_ts, save_raw=save_raw)
        except Exception as exc:
            logger.error("failed to fetch %s: %s", symbol, exc, exc_info=True)
            failed.append(symbol)
    if failed:
        logger.warning("extraction failed for: %s", failed)
    logger.info("yahoo extraction done: %d/%d symbols", len(results), len(symbols))
    return results
