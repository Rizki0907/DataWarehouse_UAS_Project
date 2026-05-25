import os
import sys
import requests
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
AIRFLOW_DATA_DIR = "/opt/airflow/crypto_dwh_data"


def _setup_env():
    """Inject project root to sys.path and set DATA_DIR before any src import."""
    os.environ["DATA_DIR"] = AIRFLOW_DATA_DIR
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    from dotenv import load_dotenv
    load_dotenv(f"{PROJECT_ROOT}/.env", override=False)
    for sub in ("raw", "processed", "sample"):
        Path(f"{AIRFLOW_DATA_DIR}/{sub}").mkdir(parents=True, exist_ok=True)


default_args = {
    "owner": "kelompok4_int24",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _extract_yahoo(**context):
    _setup_env()
    import pandas as pd
    from src.crypto_dwh.extract_yahoo import fetch_all_symbols
    from src.crypto_dwh.config import Config

    run_ts = context["ts_nodash"]
    ohlcv_dict = fetch_all_symbols(run_ts=run_ts, save_raw=True)
    combined = pd.concat(ohlcv_dict.values(), ignore_index=True)
    out_path = Config.PROCESSED_DIR / f"ohlcv_all_{run_ts}.csv"
    combined.to_csv(out_path, index=False)
    context["ti"].xcom_push(key="ohlcv_rows", value=len(combined))
    context["ti"].xcom_push(key="ohlcv_symbols", value=list(ohlcv_dict.keys()))


def _extract_fng(**context):
    _setup_env()
    from src.crypto_dwh.extract_fng import fetch_fng
    from src.crypto_dwh.config import Config

    run_ts = context["ts_nodash"]
    df = fetch_fng(run_ts=run_ts, save_raw=True)
    out_path = Config.PROCESSED_DIR / f"fng_{run_ts}.csv"
    df.to_csv(out_path, index=False)
    context["ti"].xcom_push(key="fng_rows", value=len(df))


def _transform(**context):
    _setup_env()
    from src.crypto_dwh.transform import run_transform

    run_ts = context["ts_nodash"]
    result = run_transform(run_ts=run_ts)
    context["ti"].xcom_push(key="fact_daily_rows", value=len(result["fact_market_daily"]))
    context["ti"].xcom_push(key="fact_hourly_rows", value=len(result["fact_market_hourly"]))


def _load_supabase(**context):
    _setup_env()
    from src.crypto_dwh.load_supabase import run_load

    run_ts = context["ts_nodash"]
    run_load(run_ts=run_ts)


def _refresh_materialized_views(**context):
    _setup_env()
    from src.crypto_dwh.config import Config

    supabase_url = Config.SUPABASE_URL
    supabase_key = Config.SUPABASE_KEY
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


def _data_quality(**context):
    _setup_env()
    from src.crypto_dwh.data_quality import validate_extraction_data
    run_ts = context["ts_nodash"]
    validate_extraction_data(run_ts)


def _audit_log(**context):
    _setup_env()
    from src.crypto_dwh.load_supabase import log_audit
    
    run_id = context["run_id"]
    execution_date = context["ts"]
    # Calculate duration
    dag_run = context["dag_run"]
    start_time = dag_run.start_date
    end_time = datetime.now(start_time.tzinfo)
    duration = (end_time - start_time).total_seconds()
    
    # Retrieve stats from XCom
    ohlcv_rows = context["ti"].xcom_pull(task_ids="extract_yahoo", key="ohlcv_rows") or 0
    fng_rows = context["ti"].xcom_pull(task_ids="extract_fng", key="fng_rows") or 0
    fact_daily_rows = context["ti"].xcom_pull(task_ids="transform", key="fact_daily_rows") or 0
    
    rows_extracted = {"ohlcv": ohlcv_rows, "fng": fng_rows}
    
    log_audit(
        run_id=run_id,
        execution_date=execution_date,
        status="SUCCESS",
        duration_seconds=duration,
        rows_extracted=rows_extracted,
        rows_transformed=fact_daily_rows
    )


with DAG(
    dag_id="crypto_dwh_kelompok4_int24",
    default_args=default_args,
    description="Crypto DWH Kelompok 4 INT24 - ETL pipeline: Yahoo Finance + Fear & Greed Index to Supabase.",
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "data-warehouse", "int24", "kelompok4"],
) as dag:

    t_extract_yahoo = PythonOperator(task_id="extract_yahoo", python_callable=_extract_yahoo)
    t_extract_fng   = PythonOperator(task_id="extract_fng",   python_callable=_extract_fng)
    t_data_quality  = PythonOperator(task_id="data_quality",  python_callable=_data_quality)
    t_transform     = PythonOperator(task_id="transform",     python_callable=_transform)
    t_load          = PythonOperator(task_id="load_supabase", python_callable=_load_supabase)
    t_refresh       = PythonOperator(task_id="refresh_materialized_views", python_callable=_refresh_materialized_views)
    t_audit         = PythonOperator(task_id="audit_log",     python_callable=_audit_log)

    [t_extract_yahoo, t_extract_fng] >> t_data_quality >> t_transform >> t_load >> t_refresh >> t_audit
