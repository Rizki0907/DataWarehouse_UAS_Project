import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

_SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(_SRC_PATH) not in sys.path:
    sys.path.insert(0, str(_SRC_PATH))

default_args = {
    "owner": "kelompok4_int24",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_dwh_kelompok4_int24",
    default_args=default_args,
    description="Crypto DWH Kelompok 4 INT24 - ETL pipeline: Yahoo Finance + Fear & Greed Index to Supabase.",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "data-warehouse", "int24", "kelompok4"],
) as dag:

    def _extract_yahoo(**context):
        from crypto_dwh.extract_yahoo import fetch_all_symbols
        run_ts = context["ts_nodash"]
        results = fetch_all_symbols(run_ts=run_ts)
        context["ti"].xcom_push(key="ohlcv_symbols", value=list(results.keys()))
        context["ti"].xcom_push(key="ohlcv_total_rows", value=sum(len(df) for df in results.values()))

    def _extract_fng(**context):
        from crypto_dwh.extract_fng import fetch_fng
        run_ts = context["ts_nodash"]
        df = fetch_fng(run_ts=run_ts)
        context["ti"].xcom_push(key="fng_rows", value=len(df))

    def _transform(**context):
        from crypto_dwh.transform import run_transform
        run_ts = context["ts_nodash"]
        result = run_transform(run_ts=run_ts)
        context["ti"].xcom_push(key="fact_daily_rows", value=len(result["fact_market_daily"]))
        context["ti"].xcom_push(key="fact_hourly_rows", value=len(result["fact_market_hourly"]))

    def _load_supabase(**context):
        from crypto_dwh.load_supabase import run_load
        run_ts = context["ts_nodash"]
        run_load(run_ts=run_ts)

    def _refresh_materialized_views(**context):
        import os, requests
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
        }
        views = ["mv_monthly_volatility", "mv_sentiment_vs_return", "mv_asset_performance"]
        for view in views:
            resp = requests.post(
                f"{supabase_url}/rest/v1/rpc/refresh_view",
                json={"view_name": view},
                headers=headers,
                timeout=30,
            )
            if resp.status_code not in (200, 204):
                raise RuntimeError(f"refresh {view} failed: {resp.status_code} {resp.text}")

    t_extract_yahoo = PythonOperator(task_id="extract_yahoo", python_callable=_extract_yahoo)
    t_extract_fng = PythonOperator(task_id="extract_fng", python_callable=_extract_fng)
    t_transform = PythonOperator(task_id="transform", python_callable=_transform)
    t_load = PythonOperator(task_id="load_supabase", python_callable=_load_supabase)
    t_refresh = PythonOperator(task_id="refresh_materialized_views", python_callable=_refresh_materialized_views)

    [t_extract_yahoo, t_extract_fng] >> t_transform >> t_load >> t_refresh
