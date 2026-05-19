-- ================================================================
-- SECTION 1: PARTITION MIGRATION - fact_market_hourly
-- Partition by RANGE on date_key (year-based)
-- ================================================================

ALTER TABLE fact_market_hourly DROP CONSTRAINT IF EXISTS fact_market_hourly_date_key_fkey;
ALTER TABLE fact_market_hourly DROP CONSTRAINT IF EXISTS fact_market_hourly_asset_key_fkey;
ALTER TABLE fact_market_hourly DROP CONSTRAINT IF EXISTS fact_market_hourly_pkey;

ALTER TABLE fact_market_hourly RENAME TO fact_market_hourly_old;

CREATE TABLE fact_market_hourly (
    fact_key     BIGINT       NOT NULL,
    date_key     INTEGER      NOT NULL,
    asset_key    INTEGER      NOT NULL,
    datetime_utc TIMESTAMPTZ  NOT NULL,
    hour         SMALLINT     NOT NULL,
    open         NUMERIC(18,8),
    high         NUMERIC(18,8),
    low          NUMERIC(18,8),
    close        NUMERIC(18,8),
    adj_close    NUMERIC(18,8),
    volume       NUMERIC(24,2),
    PRIMARY KEY (fact_key, date_key),
    FOREIGN KEY (date_key)  REFERENCES dim_time(date_key),
    FOREIGN KEY (asset_key) REFERENCES dim_asset(asset_key)
) PARTITION BY RANGE (date_key);

CREATE TABLE fact_market_hourly_2024
    PARTITION OF fact_market_hourly
    FOR VALUES FROM (20240101) TO (20250101);

CREATE TABLE fact_market_hourly_2025
    PARTITION OF fact_market_hourly
    FOR VALUES FROM (20250101) TO (20260101);

CREATE TABLE fact_market_hourly_2026
    PARTITION OF fact_market_hourly
    FOR VALUES FROM (20260101) TO (20270101);

INSERT INTO fact_market_hourly (fact_key, date_key, asset_key, datetime_utc, hour, open, high, low, close, adj_close, volume)
SELECT fact_key, date_key, asset_key, datetime_utc::timestamptz, hour, open, high, low, close, adj_close, volume
FROM fact_market_hourly_old;

DROP TABLE fact_market_hourly_old;


-- ================================================================
-- SECTION 2: INDEXES
-- ================================================================

CREATE INDEX IF NOT EXISTS idx_fmh_asset_date
    ON fact_market_hourly (asset_key, date_key);

CREATE INDEX IF NOT EXISTS idx_fmh_datetime
    ON fact_market_hourly (datetime_utc);

CREATE INDEX IF NOT EXISTS idx_fmd_asset_date
    ON fact_market_daily (asset_key, date_key);

CREATE INDEX IF NOT EXISTS idx_fmd_sentiment
    ON fact_market_daily (sentiment_key);

CREATE INDEX IF NOT EXISTS idx_fmd_trend
    ON fact_market_daily (trend_key);

CREATE INDEX IF NOT EXISTS idx_fmd_anomaly
    ON fact_market_daily (date_key)
    WHERE is_volume_anomaly = true;

CREATE INDEX IF NOT EXISTS idx_dim_time_year_month
    ON dim_time (year, month);


-- ================================================================
-- SECTION 3: MATERIALIZED VIEWS
-- ================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_volatility AS
SELECT
    t.year,
    t.month,
    a.symbol,
    a.name,
    ROUND(AVG(d.volatility_7d)::numeric, 6)  AS avg_volatility_7d,
    ROUND(AVG(d.daily_return)::numeric, 6)   AS avg_daily_return,
    ROUND(AVG(d.rsi_14)::numeric, 4)         AS avg_rsi_14,
    ROUND(AVG(d.close)::numeric, 4)          AS avg_close,
    COUNT(*)                                  AS trading_days
FROM fact_market_daily d
JOIN dim_time  t ON d.date_key  = t.date_key
JOIN dim_asset a ON d.asset_key = a.asset_key
WHERE d.volatility_7d IS NOT NULL
GROUP BY t.year, t.month, a.symbol, a.name
ORDER BY t.year, t.month, a.symbol;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_monthly_volatility
    ON mv_monthly_volatility (year, month, symbol);


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sentiment_vs_return AS
SELECT
    s.fng_label,
    s.fng_min,
    s.fng_max,
    a.symbol,
    ROUND(AVG(d.daily_return)::numeric, 6)   AS avg_return,
    ROUND(STDDEV(d.daily_return)::numeric, 6) AS stddev_return,
    ROUND(AVG(d.volatility_7d)::numeric, 6)  AS avg_volatility,
    COUNT(*)                                  AS observation_count
FROM fact_market_daily d
JOIN dim_sentiment s ON d.sentiment_key = s.sentiment_key
JOIN dim_asset     a ON d.asset_key     = a.asset_key
WHERE d.daily_return IS NOT NULL
GROUP BY s.fng_label, s.fng_min, s.fng_max, a.symbol
ORDER BY s.fng_min, a.symbol;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sentiment_vs_return
    ON mv_sentiment_vs_return (fng_label, symbol);


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_asset_performance AS
SELECT
    a.symbol,
    a.name,
    ROUND(MIN(d.close)::numeric, 4)          AS min_close,
    ROUND(MAX(d.close)::numeric, 4)          AS max_close,
    ROUND(AVG(d.close)::numeric, 4)          AS avg_close,
    ROUND(AVG(d.daily_return)::numeric, 6)   AS avg_daily_return,
    ROUND(AVG(d.volatility_7d)::numeric, 6)  AS avg_volatility,
    SUM(d.is_volume_anomaly::int)            AS anomaly_days,
    COUNT(*)                                  AS total_trading_days
FROM fact_market_daily d
JOIN dim_asset a ON d.asset_key = a.asset_key
GROUP BY a.symbol, a.name
ORDER BY a.symbol;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_asset_performance
    ON mv_asset_performance (symbol);


-- ================================================================
-- SECTION 4: REFRESH COMMANDS (run after each ETL cycle)
-- ================================================================

REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_volatility;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sentiment_vs_return;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_asset_performance;
