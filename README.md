# End-to-End Cryptocurrency Data Warehouse (Crypto DWH)

An end-to-end data warehouse system designed to ingest, process, and analyze cryptocurrency market data along with daily market sentiment indicators. The system is built using a Ralph Kimball dimensional modeling approach, deployed on Apache Airflow, and hosted on Supabase (PostgreSQL).

This project is developed for the Data Warehouse course, Class INT24.

---

## Team Members - Kelompok 4 (INT24)

* **Syafira Najema Putri Anisa** - Student ID: 24031554013
* **Rizki Piji Fathoni** - Student ID: 24031554029
* **Alfin Jayadi** - Student ID: 24031554082
* **Faishal Muflih Irfanu Tsaqib** - Student ID: 24031554170

---

## System Architecture & Pipeline Flow

The ETL pipeline operates on a daily schedule, executing the following phases:

1. **Extraction**: 
   * **OHLCV Market Data**: Fetches hourly price data for major assets (`BTC-USD`, `ETH-USD`, `BNB-USD`, `SOL-USD`, `ADA-USD`) from Yahoo Finance API.
   * **Sentiment Data**: Fetches the daily Crypto Fear & Greed Index from Alternative.me API.
2. **Data Quality Checks (Data Contract)**:
   * Prevents "Garbage-in, Garbage-out" by validating extracted data for negative prices, extreme null values, and datetime duplicates before transformation. 
3. **Transformation & Machine Learning**: 
   * Aggregates hourly data to daily metrics and computes technical indicators (Moving Averages, Volatility, RSI, Z-Score).
   * **Unsupervised ML**: Utilizes `IsolationForest` (Scikit-Learn) to detect multivariate market anomalies based on volume, volatility, and price changes.
   * **Supervised ML**: Utilizes a pre-trained `RandomForestClassifier` to predict tomorrow's Fear & Greed sentiment label based on today's market metrics.
4. **Load**:
   * Upserts the transformed dimensions and multi-granularity fact tables to Supabase via its REST API. Idempotent design ensures zero duplication on re-runs.
5. **OLAP & Aggregation**:
   * Triggers PostgreSQL functions via RPC to refresh Materialized Views.
6. **Audit Logging**:
   * Automatically records pipeline execution metadata (run duration, extracted row counts, execution status) into the `fact_etl_audit` table for Data Observability.

---

## Dimensional Schema Design

The system implements a classic Star Schema with two fact tables of different granularities sharing conformed dimensions:

### Conformed Dimensions
* `dim_time`: Date key, year, quarter, month, week, day of week, and weekend flags.
* `dim_asset`: Asset details (symbol, name, category).
* `dim_sentiment`: Fear and Greed index classifications.
* `dim_trend`: Combinations of Moving Average trends (bullish/bearish) and RSI signals.

### Fact Tables
* `fact_market_hourly`: Granular hourly pricing and volume data.
* `fact_market_daily`: Daily prices, technical indicators, sentiment value, dimension foreign keys, and **Machine Learning predictions** (`is_market_anomaly_ml`, `predicted_fng_label_tomorrow`).
* `fact_etl_audit`: Audit trail tracking the ETL Airflow pipeline execution duration and status.

---

## OLAP Optimization Features

To optimize query performance, the database layer (PostgreSQL) is configured with:
* **Table Partitioning**: Range partitioning applied to `fact_market_hourly` grouped by calendar year (`2024`, `2025`, `2026`).
* **Indexing**: B-Tree indices on composite keys (`asset_key`, `date_key`), foreign keys, and partial indices targeting anomaly dates.
* **Materialized Views**:
  * `mv_monthly_volatility`: Pre-computed monthly average volatility, return, and RSI per asset.
  * `mv_sentiment_vs_return`: Analytical correlation between Fear & Greed classifications and daily returns.
  * `mv_asset_performance`: Lifetime performance summary metrics for each asset.

---

## Directory Structure

```text
├── dags/
│   └── crypto_dwh_dag.py        # Apache Airflow DAG orchestrator
├── src/
│   └── crypto_dwh/
│       ├── models/
│       │   └── fng_rf_model.pkl # Trained Random Forest model for inference
│       ├── config.py            # Dynamic configuration and env management
│       ├── extract_yahoo.py     # Yahoo Finance API data extractor
│       ├── extract_fng.py       # Fear & Greed API data extractor
│       ├── data_quality.py      # Automated data quality validation layer
│       ├── train_model.py       # ML training script for Sentiment Prediction
│       ├── transform.py         # Star schema transformation & ML Inference
│       ├── load_supabase.py     # Chunked database loader & Audit Logger
│       └── pipeline.py          # Local ETL execution orchestrator
├── sql/
│   ├── 01_schema.sql            # DDL Schema for Star Schema tables
│   ├── 02_olap.sql              # Partition migration, indices, & materialized views
│   ├── 03_benchmark.sql         # Performance tuning test scripts and queries
│   ├── 04_audit_log.sql         # DDL for ETL Audit Logging
│   └── 05_ml_columns.sql        # Migration to add ML columns to fact tables
├── notebooks/
│   ├── eda.ipynb                # Exploratory Data Analysis & visualizations
│   └── atoti.ipynb              # Multidimensional OLAP cube using Atoti
└── requirements.txt             # Project dependencies list
```

---

## Getting Started (Local Development)

### 1. Prerequisites
Ensure you have Python 3.10+ and a PostgreSQL/Supabase instance ready.

### 2. Environment Setup
Clone the repository and install the dependencies:
```bash
git clone https://github.com/Rizki0907/crypto-dwh-int24.git
cd crypto-dwh-int24
python -m venv venv
source venv/Scripts/activate     # Use venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Create a `.env` file in the root directory:
```env
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_KEY=your-service-role-jwt-key
```

### 3. Database Initialization
Execute the SQL scripts in Supabase SQL Editor in the following order:
1. `sql/01_schema.sql` (Creates base DDL tables)
2. `sql/02_olap.sql` (Creates partitions, views, and functions)
3. `sql/04_audit_log.sql` (Creates audit table)
4. `sql/05_ml_columns.sql` (Adds Machine Learning columns)

### 4. Running the Pipeline
Run the full orchestrator to scrape, validate, transform (ML inference), and load data:
```bash
python -m src.crypto_dwh.pipeline
```

To re-train the Machine Learning model on the latest data:
```bash
python -m src.crypto_dwh.train_model
```

### 5. Launching Notebooks
Launch Jupyter Notebook to view analyses or explore the OLAP cube:
```bash
jupyter notebook
```

---

## Airflow Deployment

The production pipeline is deployed on Apache Airflow:
* **DAG ID**: `crypto_dwh_kelompok4_int24`
* **Schedule**: Daily at `02:00 UTC` (`09:00 WIB`)
* **Features**: Single-process task architecture, XCom data passing, Idempotency, Failure Callbacks, Data Quality Checks, and Audit Logging.
