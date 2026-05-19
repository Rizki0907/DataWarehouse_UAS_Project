-- ================================================================
-- BENCHMARK 1: Seq Scan vs Index Scan on fact_market_daily
-- Run query BEFORE and AFTER index creation to compare
-- ================================================================

-- BEFORE index: expected Seq Scan
EXPLAIN ANALYZE
SELECT
    a.symbol,
    AVG(d.close)        AS avg_close,
    AVG(d.daily_return) AS avg_return,
    AVG(d.volatility_7d) AS avg_volatility
FROM fact_market_daily d
JOIN dim_asset a ON d.asset_key = a.asset_key
WHERE d.date_key BETWEEN 20240601 AND 20241231
GROUP BY a.symbol
ORDER BY a.symbol;

-- AFTER index (idx_fmd_asset_date): expected Index Scan
-- Run same query again after executing 02_olap.sql to see improvement


-- ================================================================
-- BENCHMARK 2: Partition Pruning on fact_market_hourly
-- ================================================================

-- Full scan without filter (all partitions scanned)
EXPLAIN ANALYZE
SELECT COUNT(*), AVG(close)
FROM fact_market_hourly;

-- With year filter: only 2025 partition scanned
EXPLAIN ANALYZE
SELECT COUNT(*), AVG(close)
FROM fact_market_hourly
WHERE date_key BETWEEN 20250101 AND 20251231;

-- With asset + date filter: partition pruning + index usage
EXPLAIN ANALYZE
SELECT
    date_key,
    AVG(close) AS avg_hourly_close,
    SUM(volume) AS total_volume
FROM fact_market_hourly
WHERE asset_key = 1
  AND date_key BETWEEN 20250101 AND 20250630
GROUP BY date_key
ORDER BY date_key;


-- ================================================================
-- BENCHMARK 3: Materialized View vs Direct Query
-- ================================================================

-- Direct query (slower - computes on the fly)
EXPLAIN ANALYZE
SELECT
    t.year,
    t.month,
    a.symbol,
    ROUND(AVG(d.volatility_7d)::numeric, 6) AS avg_volatility_7d,
    ROUND(AVG(d.daily_return)::numeric, 6)  AS avg_daily_return,
    COUNT(*) AS trading_days
FROM fact_market_daily d
JOIN dim_time  t ON d.date_key  = t.date_key
JOIN dim_asset a ON d.asset_key = a.asset_key
WHERE d.volatility_7d IS NOT NULL
GROUP BY t.year, t.month, a.symbol, a.name
ORDER BY t.year, t.month, a.symbol;

-- Materialized view query (faster - pre-computed)
EXPLAIN ANALYZE
SELECT year, month, symbol, avg_volatility_7d, avg_daily_return, trading_days
FROM mv_monthly_volatility
ORDER BY year, month, symbol;


-- ================================================================
-- BENCHMARK 4: Partial Index for Volume Anomaly Detection
-- ================================================================

-- Without partial index: full scan
EXPLAIN ANALYZE
SELECT d.date_key, a.symbol, d.volume_zscore
FROM fact_market_daily d
JOIN dim_asset a ON d.asset_key = a.asset_key
WHERE d.is_volume_anomaly = true
ORDER BY d.date_key;


-- ================================================================
-- ANALYTIC QUERIES (untuk laporan & EDA)
-- ================================================================

-- Q1: Korelasi FNG vs Return per koin
SELECT
    s.fng_label,
    a.symbol,
    ROUND(AVG(d.daily_return)::numeric, 6)   AS avg_return,
    ROUND(STDDEV(d.daily_return)::numeric, 6) AS return_std,
    COUNT(*)                                  AS n
FROM fact_market_daily d
JOIN dim_sentiment s ON d.sentiment_key = s.sentiment_key
JOIN dim_asset     a ON d.asset_key     = a.asset_key
WHERE d.daily_return IS NOT NULL
GROUP BY s.fng_label, s.fng_min, a.symbol
ORDER BY s.fng_min, a.symbol;

-- Q2: Monthly rolling volatility BTC vs ETH
SELECT
    year, month, symbol,
    avg_volatility_7d,
    avg_daily_return,
    trading_days
FROM mv_monthly_volatility
WHERE symbol IN ('BTC-USD', 'ETH-USD')
ORDER BY year, month, symbol;

-- Q3: Volume anomaly calendar heatmap
SELECT
    t.year,
    t.month,
    a.symbol,
    COUNT(*) AS anomaly_count
FROM fact_market_daily d
JOIN dim_time  t ON d.date_key  = t.date_key
JOIN dim_asset a ON d.asset_key = a.asset_key
WHERE d.is_volume_anomaly = true
GROUP BY t.year, t.month, a.symbol
ORDER BY t.year, t.month, a.symbol;

-- Q4: RSI Signal distribution per asset
SELECT
    a.symbol,
    tr.rsi_signal,
    COUNT(*) AS days,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY a.symbol), 2) AS pct
FROM fact_market_daily d
JOIN dim_asset a  ON d.asset_key = a.asset_key
JOIN dim_trend tr ON d.trend_key  = tr.trend_key
GROUP BY a.symbol, tr.rsi_signal
ORDER BY a.symbol, tr.rsi_signal;

-- Q5: Overall asset performance summary
SELECT
    symbol, name,
    min_close, max_close, avg_close,
    avg_daily_return, avg_volatility,
    anomaly_days, total_trading_days,
    ROUND(anomaly_days * 100.0 / total_trading_days, 2) AS anomaly_pct
FROM mv_asset_performance
ORDER BY avg_daily_return DESC;
