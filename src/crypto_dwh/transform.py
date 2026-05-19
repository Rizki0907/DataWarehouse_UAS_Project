import pandas as pd


def build_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    raise NotImplementedError("phase 2")


def build_dim_asset(symbols: list[str]) -> pd.DataFrame:
    raise NotImplementedError("phase 2")


def build_dim_sentiment(df_fng: pd.DataFrame) -> pd.DataFrame:
    raise NotImplementedError("phase 2")


def build_fact_market_data(ohlcv_dict: dict, df_fng: pd.DataFrame) -> pd.DataFrame:
    raise NotImplementedError("phase 2")


def run_transform(extraction_result: dict) -> dict:
    raise NotImplementedError("phase 2")
