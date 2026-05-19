import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from .config import Config

logger = logging.getLogger(__name__)

_LOAD_ORDER = [
    "dim_time",
    "dim_asset",
    "dim_sentiment",
    "dim_trend",
    "fact_market_hourly",
    "fact_market_daily",
]


def _make_headers() -> dict:
    return {
        "apikey": Config.SUPABASE_KEY,
        "Authorization": f"Bearer {Config.SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


def _clean_value(v):
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        if np.isnan(v) or np.isinf(v):
            return None
        if v == int(v):
            return int(v)
        return float(v)
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        if v == int(v):
            return int(v)
        return v
    return v


def _to_records(df: pd.DataFrame) -> list[dict]:
    records = df.to_dict(orient="records")
    return [{k: _clean_value(v) for k, v in row.items()} for row in records]


def _upsert_chunk(table: str, records: list[dict], headers: dict) -> None:
    url = f"{Config.SUPABASE_URL}/rest/v1/{table}"
    last_exc: Optional[Exception] = None
    for attempt in range(1, Config.REQUEST_MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=records, headers=headers, timeout=Config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            return
        except requests.HTTPError as exc:
            last_exc = exc
            wait = Config.REQUEST_BACKOFF_S * (2 ** (attempt - 1))
            logger.warning("upsert %s attempt %d/%d status=%s, retry in %.1fs",
                           table, attempt, Config.REQUEST_MAX_RETRIES, exc.response.status_code, wait)
            time.sleep(wait)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            wait = Config.REQUEST_BACKOFF_S * (2 ** (attempt - 1))
            logger.warning("upsert %s attempt %d/%d network error, retry in %.1fs",
                           table, attempt, Config.REQUEST_MAX_RETRIES, wait)
            time.sleep(wait)
    raise requests.ConnectionError(f"upsert {table} failed after {Config.REQUEST_MAX_RETRIES} attempts") from last_exc


def upsert_table(table_name: str, df: pd.DataFrame, chunk_size: int = 500) -> None:
    records = _to_records(df)
    total = len(records)
    headers = _make_headers()
    n_chunks = math.ceil(total / chunk_size)
    logger.info("loading %s | %d rows | %d chunks", table_name, total, n_chunks)
    for i in range(n_chunks):
        chunk = records[i * chunk_size: (i + 1) * chunk_size]
        _upsert_chunk(table_name, chunk, headers)
        logger.info("  %s chunk %d/%d done (%d rows)", table_name, i + 1, n_chunks, len(chunk))
    logger.info("done loading %s", table_name)


def _load_latest_parquet(name: str) -> pd.DataFrame:
    files = sorted(Config.PROCESSED_DIR.glob(f"{name}_*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet found for '{name}' in {Config.PROCESSED_DIR}")
    path = files[-1]
    logger.info("reading %s", path.name)
    return pd.read_parquet(path)


def run_load(transform_result: Optional[dict] = None, run_ts: Optional[str] = None) -> None:
    run_ts = run_ts or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info("load started | run_ts=%s", run_ts)

    if not Config.SUPABASE_URL or not Config.SUPABASE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment")

    tables: dict[str, pd.DataFrame] = {}
    if transform_result:
        for name in _LOAD_ORDER:
            if name in transform_result:
                tables[name] = transform_result[name]
    else:
        for name in _LOAD_ORDER:
            tables[name] = _load_latest_parquet(name)

    for name in _LOAD_ORDER:
        if name in tables:
            upsert_table(name, tables[name])
        else:
            logger.warning("table %s not found, skipping", name)

    logger.info("load complete | tables loaded: %s", list(tables.keys()))


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    run_load()
