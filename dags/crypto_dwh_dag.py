import sys
import os
import requests
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = str(Path(__file__).resolve().parents[1])

default_args = {
    "owner": "kelompok4_int24",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_etl_pipeline(**context):
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)

    from src.crypto_dwh.pipeline import run_extraction
    from src.crypto_dwh.transform import run_transform
    from src.crypto_dwh.load_supabase import run_load

    run_ts = context["ts_nodash"]

    # Phase 1: Extract (returns in-memory dict, no filesystem dependency)
    extraction_result = run_extraction(run_ts=run_ts, save_raw=False, save_snapshots=False)

    # Phase 2: Transform (receives in-memory data directly)
    transform_result = run_transform(extraction_result=extraction_result, run_ts=run_ts)

    # Phase 3: Load to Supabase
    run_load(run_ts=run_ts, transform_result=transform_result)

    # Push summary to XCom
    context["ti"].xcom_push(key="fact_daily_rows", value=len(transform_result["fact_market_daily"]))
    context["ti"].xcom_push(key="fact_hourly_rows", value=len(transform_result["fact_market_hourly"]))


def _refresh_materialized_views(**context):
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    from dotenv import load_dotenv
    load_dotenv(f"{PROJECT_ROOT}/.env", override=False)

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


with DAG(
    dag_id="crypto_dwh_kelompok4_int24",
    default_args=default_args,
    description="Crypto DWH Kelompok 4 INT24 - ETL pipeline: Yahoo Finance + Fear & Greed Index to Supabase.",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "data-warehouse", "int24", "kelompok4"],
) as dag:

    t_etl = PythonOperator(
        task_id="etl_pipeline",
        python_callable=_run_etl_pipeline,
    )

    t_refresh = PythonOperator(
        task_id="refresh_materialized_views",
        python_callable=_refresh_materialized_views,
    )

    t_etl >> t_refresh
