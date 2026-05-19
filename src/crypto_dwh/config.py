import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / ".env", override=False)


def _default_end() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _default_start(days_back: int = 729) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    return dt.strftime("%Y-%m-%d")


class Config:
    SYMBOLS: list[str] = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "ADA-USD"]

    START_DATE: str = os.getenv("START_DATE", _default_start(729))
    END_DATE: str = os.getenv("END_DATE", _default_end())

    YF_INTERVAL: str = os.getenv("YF_INTERVAL", "1h")
    YF_CHUNK_DAYS: int = int(os.getenv("YF_CHUNK_DAYS", "59"))

    FNG_BASE_URL: str = "https://api.alternative.me/fng/"
    FNG_LIMIT: int = int(os.getenv("FNG_LIMIT", "730"))

    YF_BASE_URL: str = "https://query1.finance.yahoo.com/v8/finance/chart/"
    YF_HEADERS: dict = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    REQUEST_MAX_RETRIES: int = int(os.getenv("REQUEST_MAX_RETRIES", "3"))
    REQUEST_BACKOFF_S: float = float(os.getenv("REQUEST_BACKOFF_S", "2.0"))

    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")

    _DATA_DIR = Path(os.getenv("DATA_DIR", str(_PROJECT_ROOT / "data")))
    RAW_DIR: Path = _DATA_DIR / "raw"
    PROCESSED_DIR: Path = _DATA_DIR / "processed"
    SAMPLE_DIR: Path = _DATA_DIR / "sample"

    for _d in (RAW_DIR, PROCESSED_DIR, SAMPLE_DIR):
        _d.mkdir(parents=True, exist_ok=True)
