import logging
import pandas as pd
from pathlib import Path
from typing import Optional

from .config import Config

logger = logging.getLogger(__name__)

class DataQualityError(Exception):
    """Raised when data quality validation fails."""
    pass

def validate_extraction_data(run_ts: str) -> None:
    """
    Mengecek kualitas data hasil extract sebelum diproses lebih lanjut.
    Mencegah garbage data masuk ke Data Warehouse.
    """
    logger.info("Memulai Data Quality Checks untuk run_ts=%s", run_ts)
    
    # 1. Cek File Ekstraksi Yahoo (OHLCV)
    ohlcv_path = Config.PROCESSED_DIR / f"ohlcv_all_{run_ts}.csv"
    if not ohlcv_path.exists():
        raise DataQualityError(f"File OHLCV tidak ditemukan: {ohlcv_path}")
        
    df_ohlcv = pd.read_csv(ohlcv_path)
    
    if df_ohlcv.empty:
        raise DataQualityError("Data OHLCV kosong!")
        
    # Validasi: Harga dan volume tidak boleh negatif
    for col in ["open", "high", "low", "close", "volume"]:
        if (df_ohlcv[col] < 0).any():
            raise DataQualityError(f"Terdapat nilai negatif pada kolom {col} di data OHLCV!")
            
    # Validasi: Tidak boleh ada duplikasi datetime per asset
    duplikat = df_ohlcv.duplicated(subset=["symbol", "datetime_utc"]).sum()
    if duplikat > 0:
        raise DataQualityError(f"Terdapat {duplikat} baris duplikat pada data OHLCV (symbol + datetime)!")
        
    # Validasi: Missing values pada kolom penting
    nulls = df_ohlcv[["close", "volume"]].isnull().sum().sum()
    if nulls > 0:
        logger.warning("Data Quality Warning: Ditemukan %d nilai null pada kolom close/volume di OHLCV.", nulls)
        
    # 2. Cek File Ekstraksi FnG
    fng_path = Config.PROCESSED_DIR / f"fng_{run_ts}.csv"
    if not fng_path.exists():
        raise DataQualityError(f"File FnG tidak ditemukan: {fng_path}")
        
    df_fng = pd.read_csv(fng_path)
    
    if df_fng.empty:
        raise DataQualityError("Data FnG kosong!")
        
    # Validasi: Nilai FnG harus berada di range 0-100
    if not df_fng["fng_value"].between(0, 100).all():
        raise DataQualityError("Nilai Fear and Greed Index berada di luar batas normal (0-100)!")
        
    logger.info("✅ Data Quality Checks Berhasil! Data aman untuk diproses.")

if __name__ == "__main__":
    # Test script run
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1:
        validate_extraction_data(sys.argv[1])
