import pandas as pd


def upsert_dim_time(df: pd.DataFrame) -> None:
    raise NotImplementedError("phase 3")


def upsert_dim_asset(df: pd.DataFrame) -> None:
    raise NotImplementedError("phase 3")


def upsert_dim_sentiment(df: pd.DataFrame) -> None:
    raise NotImplementedError("phase 3")


def upsert_fact_market_data(df: pd.DataFrame) -> None:
    raise NotImplementedError("phase 3")


def run_load(transform_result: dict) -> None:
    raise NotImplementedError("phase 3")
