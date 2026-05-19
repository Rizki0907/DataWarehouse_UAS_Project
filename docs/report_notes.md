# Report Notes & Design Decisions

## Project Title
Design and Implementation of a Cryptocurrency Data Warehouse  
for Volatility and Sentiment-Driven Market Analysis

---

## Architecture: Kimball Dimensional Model

### Star Schema
```
fact_market_data
   ├── FK → dim_time   (date_key)
   ├── FK → dim_asset  (asset_key)
   ├── FK → dim_sentiment (sentiment_key)
   └── FK → dim_trend  (trend_key)
```

### Grain
One row per **asset × date** (daily granularity).

---

## Data Sources

| Source | Type | Frequency | Volume |
|--------|------|-----------|--------|
| Yahoo Finance v8/chart API | OHLCV market data | Daily (1d interval) | ~730 rows × 5 symbols |
| Alternative.me FNG API | Sentiment index (0–100) | Daily | ~730 rows |

---

## ETL Phases

| Phase | Module | Status |
|-------|--------|--------|
| Extract – OHLCV | `extract_yahoo.py` | ✅ Phase 1 complete |
| Extract – FNG | `extract_fng.py` | ✅ Phase 1 complete |
| Transform | `transform.py` | 🔲 Phase 2 pending |
| Load | `load_supabase.py` | 🔲 Phase 3 pending |
| Airflow DAG | `dags/crypto_dwh_dag.py` | 🔲 Phase 4 pending |

---

## Indicator Definitions

| Indicator | Formula / Method |
|-----------|-----------------|
| `daily_return` | `(close - close.shift(1)) / close.shift(1)` |
| `ma7` | 7-day rolling mean of `close` |
| `ma30` | 30-day rolling mean of `close` |
| `volatility_7d` | 7-day rolling std of `daily_return` |
| `rsi_14` | Wilder's RSI over 14 periods |
| `trend_label` | `bullish` / `bearish` / `neutral` based on `ma7 vs ma30` |
| `volume_zscore` | Z-score of `volume` over trailing 30d window |
| `is_volume_anomaly` | `abs(volume_zscore) > 2` |
| `rsi_signal` | `oversold` (<30) / `overbought` (>70) / `neutral` |

---

## PostgreSQL Advanced Features (Planned)

- **Partitioning**: `fact_market_data` range-partitioned by year on `date_key`.
- **Indexes**: B-tree on `(symbol_key, date_key)`, GIN partial index for anomalies.
- **Materialized Views**:  
  - `mv_monthly_volatility` – monthly vol aggregated per asset.  
  - `mv_sentiment_price_corr` – rolling correlation of FNG vs daily_return.
- **Benchmark queries**: `EXPLAIN ANALYZE` before/after index creation.

---

## Open Questions / Decisions

1. Should hourly data be fetched for the most recent 730-day window only,
   then aggregated to daily, or use daily from the start?  
   → Current decision: **daily (1d)** for the full 2-year range.

2. Supabase upsert key for `fact_market_data`?  
   → Candidate: composite `(date_key, asset_key)`.

3. Atoti cube dimensions?  
   → Time hierarchy, Asset, Sentiment bucket, Trend label.
