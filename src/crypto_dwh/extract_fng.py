import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from .config import Config

logger = logging.getLogger(__name__)

_CLASSIFICATION_MAP: dict[str, str] = {
    "Extreme Fear": "extreme_fear",
    "Fear": "fear",
    "Neutral": "neutral",
    "Greed": "greed",
    "Extreme Greed": "extreme_greed",
}


def _get(limit: int) -> dict:
    params = {"limit": limit, "format": "json"}
    last_exc: Optional[Exception] = None
    for attempt in range(1, Config.REQUEST_MAX_RETRIES + 1):
        try:
            resp = requests.get(Config.FNG_BASE_URL, params=params, timeout=Config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            wait = Config.REQUEST_BACKOFF_S * (2 ** (attempt - 1))
            logger.warning("fng attempt %d/%d failed, retrying in %.1fs", attempt, Config.REQUEST_MAX_RETRIES, wait)
            time.sleep(wait)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                wait = Config.REQUEST_BACKOFF_S * (2 ** (attempt - 1))
                logger.warning("fng rate limited (429), retrying in %.1fs", wait)
                time.sleep(wait)
                last_exc = exc
            else:
                raise
    raise requests.ConnectionError(f"all {Config.REQUEST_MAX_RETRIES} fng retries failed") from last_exc


def _parse(data: dict) -> pd.DataFrame:
    try:
        records = data["data"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"unexpected fng response structure: {exc}") from exc
    if not records:
        raise ValueError("fng api returned empty data")
    rows = []
    for record in records:
        try:
            ts = int(record["timestamp"])
            val = int(record["value"])
            cls = record.get("value_classification", "")
            rows.append(
                {
                    "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                    "fng_value": val,
                    "fng_classification": _CLASSIFICATION_MAP.get(cls, cls.lower().replace(" ", "_")),
                }
            )
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("skipping malformed fng record: %s", exc)
    df = pd.DataFrame(rows)
    df.drop_duplicates(subset=["date"], keep="first", inplace=True)
    df.sort_values("date", ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)
    logger.info("parsed %d fng records (%s -> %s)", len(df), df["date"].iloc[0], df["date"].iloc[-1])
    return df


def _dump_json(data: dict, run_ts: str) -> Path:
    path = Config.RAW_DIR / f"fng_{run_ts}.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    logger.info("saved %s", path)
    return path


def _dump_csv(df: pd.DataFrame, run_ts: str) -> Path:
    path = Config.RAW_DIR / f"fng_{run_ts}.csv"
    df.to_csv(path, index=False)
    logger.info("saved %s", path)
    return path


def fetch_fng(limit: Optional[int] = None, run_ts: Optional[str] = None, save_raw: bool = True) -> pd.DataFrame:
    """Fetch the Alternative.me Fear & Greed Index, returning a cleaned DataFrame."""
    limit = limit or Config.FNG_LIMIT
    run_ts = run_ts or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info("fetching fng limit=%d", limit)
    raw = _get(limit)
    if save_raw:
        _dump_json(raw, run_ts)
    df = _parse(raw)
    if save_raw:
        _dump_csv(df, run_ts)
    return df
