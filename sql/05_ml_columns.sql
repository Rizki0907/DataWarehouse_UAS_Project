-- SECTION 6: MACHINE LEARNING COLUMNS
-- Menambahkan kolom baru hasil prediksi Machine Learning (Sprint 2) ke tabel fakta

ALTER TABLE fact_market_daily 
ADD COLUMN IF NOT EXISTS is_market_anomaly_ml BOOLEAN,
ADD COLUMN IF NOT EXISTS predicted_fng_label_tomorrow VARCHAR(50);
