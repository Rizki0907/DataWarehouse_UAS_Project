import logging
import pandas as pd
from pathlib import Path
import pickle
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

from .config import Config

logger = logging.getLogger(__name__)

def train_and_save_model():
    """
    Melatih model Random Forest untuk memprediksi sentimen Fear & Greed hari esok
    berdasarkan data historis hari ini.
    """
    logger.info("Memulai proses training model ML...")
    
    # 1. Load data historis (kita pakai data yang sudah ditransformasi)
    files = sorted(Config.PROCESSED_DIR.glob("fact_market_daily_*.parquet"))
    if not files:
        logger.error("Data fact_market_daily parquet belum ada. Jalankan pipeline ETL minimal 1x.")
        return
        
    df_fact = pd.read_parquet(files[-1])
    
    dim_sentiment_files = sorted(Config.PROCESSED_DIR.glob("dim_sentiment_*.parquet"))
    if not dim_sentiment_files:
        logger.error("Data dim_sentiment parquet belum ada.")
        return
        
    df_sentiment = pd.read_parquet(dim_sentiment_files[-1])
    
    # Gabungkan dengan label sentimen
    df = df_fact.merge(df_sentiment[["sentiment_key", "fng_classification"]], on="sentiment_key", how="left")
    
    # 2. Persiapan Features (X) dan Target (y)
    # Kita ingin memprediksi fng_classification HARI ESOK (t+1)
    # Jadi kita shift label ke belakang 1 baris per asset
    df["target_fng_tomorrow"] = df.groupby("asset_key")["fng_classification"].shift(-1)
    
    # Drop baris terakhir yang tidak punya label hari esok
    df = df.dropna(subset=["target_fng_tomorrow", "daily_return", "volatility_7d", "rsi_14"])
    
    features = ["daily_return", "volatility_7d", "rsi_14", "volume_zscore", "fng_value"]
    X = df[features]
    y = df["target_fng_tomorrow"]
    
    if len(df) < 100:
        logger.warning("Data terlalu sedikit untuk dilatih (%d baris). Tapi tetap dilanjut untuk dummy.", len(df))
        
    # 3. Train Test Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 4. Inisialisasi dan Train Model Random Forest
    rf_model = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    rf_model.fit(X_train, y_train)
    
    # 5. Evaluasi Sederhana
    y_pred = rf_model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    logger.info("Akurasi Model Prediksi Fear & Greed: %.2f%%", acc * 100)
    
    # 6. Simpan Model
    model_dir = Path(__file__).resolve().parent / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    
    model_path = model_dir / "fng_rf_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(rf_model, f)
        
    logger.info("✅ Model sukses disimpan ke: %s", model_path)

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    train_and_save_model()
