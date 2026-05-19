from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "crypto_dwh_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_dwh_pipeline",
    default_args=default_args,
    description="Crypto DWH: extract, transform, load to Supabase.",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crypto", "data-warehouse"],
) as dag:

    def _extract_yahoo(**context):
        from crypto_dwh.extract_yahoo import fetch_all_symbols
        run_ts = context["ts_nodash"]
        results = fetch_all_symbols(run_ts=run_ts)
        context["ti"].xcom_push(key="ohlcv_symbols", value=list(results.keys()))

    def _extract_fng(**context):
        from crypto_dwh.extract_fng import fetch_fng
        run_ts = context["ts_nodash"]
        df = fetch_fng(run_ts=run_ts)
        context["ti"].xcom_push(key="fng_rows", value=len(df))

    def _transform(**context):
        raise NotImplementedError("phase 2 pending")

    def _load_supabase(**context):
        raise NotImplementedError("phase 3 pending")

    t_extract_yahoo = PythonOperator(task_id="extract_yahoo", python_callable=_extract_yahoo)
    t_extract_fng = PythonOperator(task_id="extract_fng", python_callable=_extract_fng)
    t_transform = PythonOperator(task_id="transform", python_callable=_transform)
    t_load = PythonOperator(task_id="load_supabase", python_callable=_load_supabase)

    [t_extract_yahoo, t_extract_fng] >> t_transform >> t_load
