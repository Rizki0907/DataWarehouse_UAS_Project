import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from .config import Config
from .extract_fng import fetch_fng
from .extract_yahoo import fetch_all_symbols

_handler = logging.StreamHandler(sys.stdout)
if hasattr(_handler.stream, "reconfigure"):
    _handler.stream.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    handlers=[_handler],
)
logger = logging.getLogger(__name__)


def _snapshot_ohlcv(ohlcv_dict: dict[str, pd.DataFrame], run_ts: str) -> Optional[Path]:
    if not ohlcv_dict:
        logger.warning("no ohlcv data to snapshot")
        return None
    combined = pd.concat(ohlcv_dict.values(), ignore_index=True)
    path = Config.PROCESSED_DIR / f"ohlcv_all_{run_ts}.csv"
    combined.to_csv(path, index=False)
    logger.info("ohlcv snapshot saved -> %s (%d rows)", path, len(combined))
    return path


def _snapshot_fng(df: pd.DataFrame, run_ts: str) -> Path:
    path = Config.PROCESSED_DIR / f"fng_{run_ts}.csv"
    df.to_csv(path, index=False)
    logger.info("fng snapshot saved -> %s (%d rows)", path, len(df))
    return path


def run_extraction(
    symbols: Optional[list[str]] = None,
    run_ts: Optional[str] = None,
    save_raw: bool = True,
    save_snapshots: bool = True,
) -> dict:
    """Run the full extraction phase: Yahoo OHLCV + Fear & Greed Index."""
    run_ts = run_ts or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info("extraction started | run_ts=%s", run_ts)
    logger.info("date range: %s -> %s | symbols: %s", Config.START_DATE, Config.END_DATE, symbols or Config.SYMBOLS)

    ohlcv_dict = fetch_all_symbols(symbols=symbols, run_ts=run_ts, save_raw=save_raw)
    df_fng = fetch_fng(run_ts=run_ts, save_raw=save_raw)

    ohlcv_path = _snapshot_ohlcv(ohlcv_dict, run_ts) if save_snapshots else None
    fng_path = _snapshot_fng(df_fng, run_ts) if save_snapshots else None

    total_rows = sum(len(df) for df in ohlcv_dict.values())
    logger.info("extraction complete | ohlcv: %d symbols %d rows | fng: %d rows", len(ohlcv_dict), total_rows, len(df_fng))

    return {
        "run_ts": run_ts,
        "ohlcv": ohlcv_dict,
        "fng": df_fng,
        "ohlcv_snapshot_path": ohlcv_path,
        "fng_snapshot_path": fng_path,
    }


if __name__ == "__main__":
    result = run_extraction()
    print(f"run_ts: {result['run_ts']}")
    for sym, df in result["ohlcv"].items():
        print(f"  {sym}: {len(df)} rows ({df['date'].min()} -> {df['date'].max()})")
    print(f"fng: {len(result['fng'])} rows")
