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

1. **Extraction (Phase 1)**: 
   * **OHLCV Market Data**: Fetches hourly price data (Open, High, Low, Close, Volume) for major assets (`BTC-USD`, `ETH-USD`, `BNB-USD`, `SOL-USD`, `ADA-USD`) from the Yahoo Finance API using chunked HTTP requests.
   * **Sentiment Data**: Fetches the daily Crypto Fear & Greed Index from the Alternative.me API.
2. **Transformation (Phase 2)**: 
   * Aggregates hourly candlestick data to daily metrics.
   * Computes technical indicators: 7-day and 30-day Moving Averages (MA), 7-day rolling Volatility, 14-period Relative Strength Index (RSI), Volume Z-score, and classifies Volume Anomalies.
   * Joins the technical indicators with the sentiment index on the date dimension.
3. **Load (Phase 3)**:
   * Upserts the transformed dimensions and multi-granularity fact tables to Supabase via its REST API using chunked, retriable requests.
4. **OLAP & Aggregation (Phase 4)**:
   * Triggers materialized view refreshes in PostgreSQL to update pre-computed analytical views.

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
* `fact_market_daily`: Daily prices, technical indicators, sentiment value, and dimension foreign keys.

---

## OLAP Optimization Features

To optimize query performance, the database layer (PostgreSQL) is configured with:
* **Partitioning**: Range partitioning applied to `fact_market_hourly` grouped by calendar year (`2024`, `2025`, `2026`).
* **Indexing**: B-Tree indices on composite keys (`asset_key`, `date_key`), foreign keys, and a partial index specifically targeting volume anomaly dates.
* **Materialized Views**:
  * `mv_monthly_volatility`: Pre-computed monthly average volatility, return, and RSI per asset.
  * `mv_sentiment_vs_return`: Analytical correlation between Fear & Greed classifications and daily returns.
  * `mv_asset_performance`: Lifetime performance summary metrics for each asset.

---

## Data Analysis & OLAP Cubes

The analytical layer provides detailed explorations located in the `notebooks/` directory:

### 1. Exploratory Data Analysis (EDA) - [`notebooks/eda.ipynb`](file:///notebooks/eda.ipynb)
Provides comprehensive visualizations and statistical evaluations:
* Missing value evaluation and dataset consistency checks.
* Descriptive statistics per asset symbol.
* Multi-asset price trends, 7D volatility, and RSI indicators.
* Fear & Greed Index distribution and its correlation (Pearson score) to daily asset returns.
* Volume anomaly calculations based on Z-score thresholds.

### 2. Atoti DataMart & OLAP Cube - [`notebooks/atoti.ipynb`](file:///notebooks/atoti.ipynb)
Spins up a multidimensional OLAP Cube using **Atoti** with five defined analytical targets:
* **T1: Volatility Profile**: Rata-rata 7D volatility and daily returns rolled up by year, quarter, and asset.
* **T2: Sentiment vs. Returns**: Correlation check mapping sentiment categories to price return impacts.
* **T3: Trend Distribution**: Number of days in bullish vs. bearish trends sliced across the calendar time hierarchy.
* **T4: Volume Anomaly & Price Impact**: Analysis of average returns and volumes during anomalies.
* **T5: RSI Momentum**: Heatmap distribution cross-referencing RSI signals and trend labels per asset.
* **Interactive Widgets**: Interactive pivot tables and dashboard server instance link enabled for visual drill-downs.

---

## Directory Structure

```text
├── dags/
│   └── crypto_dwh_dag.py        # Apache Airflow DAG definition
├── src/
│   └── crypto_dwh/
│       ├── config.py            # Dynamic configuration and env management
│       ├── extract_yahoo.py     # Yahoo Finance API data extractor
│       ├── extract_fng.py       # Fear & Greed API data extractor
│       ├── transform.py         # Star schema transformation & indicators
│       ├── load_supabase.py     # Chunked database loader
│       └── pipeline.py          # Local ETL execution orchestrator
├── sql/
│   ├── 01_schema.sql            # DDL Schema for Star Schema tables
│   ├── 02_olap.sql              # Partition migration, indices, & materialized views
│   └── 03_benchmark.sql         # Performance tuning test scripts and queries
├── notebooks/
│   ├── eda.ipynb                # Exploratory Data Analysis & visualizations (Completed)
│   └── atoti.ipynb              # Multidimensional OLAP cube using Atoti (Completed)
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
2. Run the Python ETL pipeline (see below) to populate the database.
3. `sql/02_olap.sql` (Performs partition migrations, creates indices, and materialized views)

### 4. Running the Local Pipeline
Run the full orchestrator to scrape, transform, and load data:
```bash
python -m src.crypto_dwh.pipeline
```

### 5. Launching Notebooks
Launch Jupyter Notebook to view analyses or explore the OLAP cube:
```bash
jupyter notebook
```
Open `notebooks/eda.ipynb` or `notebooks/atoti.ipynb` to execute the code.

---

## Airflow Deployment

The production pipeline is deployed on Apache Airflow:
* **DAG ID**: `crypto_dwh_kelompok4_int24`
* **Schedule**: Daily at `02:00 UTC` (`09:00 WIB`)
* **Environment Variables**: Managed via local `.env` inside the DAG folder.
