CREATE TABLE IF NOT EXISTS dim_time (
    date_key   INTEGER PRIMARY KEY,
    date       DATE        NOT NULL,
    year       SMALLINT    NOT NULL,
    quarter    SMALLINT    NOT NULL,
    month      SMALLINT    NOT NULL,
    week       SMALLINT    NOT NULL,
    day_of_week SMALLINT   NOT NULL,
    day_name   VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN     NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_asset (
    asset_key  INTEGER     PRIMARY KEY,
    symbol     VARCHAR(20) NOT NULL UNIQUE,
    name       VARCHAR(100) NOT NULL,
    category   VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_sentiment (
    sentiment_key    INTEGER     PRIMARY KEY,
    fng_classification VARCHAR(20) NOT NULL UNIQUE,
    fng_label        VARCHAR(50) NOT NULL,
    fng_min          SMALLINT    NOT NULL,
    fng_max          SMALLINT    NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_trend (
    trend_key   INTEGER     PRIMARY KEY,
    trend_label VARCHAR(20) NOT NULL,
    rsi_signal  VARCHAR(20) NOT NULL,
    UNIQUE (trend_label, rsi_signal)
);

CREATE TABLE IF NOT EXISTS fact_market_hourly (
    fact_key     BIGINT       PRIMARY KEY,
    date_key     INTEGER      NOT NULL REFERENCES dim_time(date_key),
    asset_key    INTEGER      NOT NULL REFERENCES dim_asset(asset_key),
    datetime_utc TIMESTAMPTZ  NOT NULL,
    hour         SMALLINT     NOT NULL,
    open         NUMERIC(18,8),
    high         NUMERIC(18,8),
    low          NUMERIC(18,8),
    close        NUMERIC(18,8),
    adj_close    NUMERIC(18,8),
    volume       NUMERIC(24,2)
);

CREATE TABLE IF NOT EXISTS fact_market_daily (
    fact_key          BIGINT       PRIMARY KEY,
    date_key          INTEGER      NOT NULL REFERENCES dim_time(date_key),
    asset_key         INTEGER      NOT NULL REFERENCES dim_asset(asset_key),
    sentiment_key     INTEGER      REFERENCES dim_sentiment(sentiment_key),
    trend_key         INTEGER      REFERENCES dim_trend(trend_key),
    open              NUMERIC(18,8),
    high              NUMERIC(18,8),
    low               NUMERIC(18,8),
    close             NUMERIC(18,8),
    volume            NUMERIC(24,2),
    fng_value         SMALLINT,
    daily_return      NUMERIC(10,6),
    ma7               NUMERIC(18,8),
    ma30              NUMERIC(18,8),
    volatility_7d     NUMERIC(10,6),
    rsi_14            NUMERIC(8,4),
    volume_zscore     NUMERIC(10,6),
    is_volume_anomaly BOOLEAN
);
