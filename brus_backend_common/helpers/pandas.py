import logging
from datetime import datetime
from typing import List, Callable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def convert_timestamp_df(dt: datetime) -> np.datetime64:
    """Simply converts datetime's to datetime64[us] for dataframes"""
    return np.datetime64(dt).astype("datetime64[us]")


def check_dataframe_diff(
    new_data: pd.DataFrame,
    current_data: pd.DataFrame,
    del_cols: List[str],
    sort_cols: List[str],
    lambda_funcs: List[tuple[str, Callable]] = None,
    date_format: str = "%m/%d/%Y",
) -> bool:
    """Checks if 2 dataframes (the new data and the existing data for a model) are different.

    Args:
        new_data: dataframe containing the new data to compare
        current_data: dataframe containing the current data to compare (pulled from the model.to_pandas_df())
        del_cols: An array containing the columns to delete from the existing data (usually id and foreign keys)
        sort_cols: An array containing the columns to sort on
        lambda_funcs: An array of tuples [(column to update, transformative lambda taking in a row argument)]
                      that will be processed in the order provided. This is only applied to new_data.
        date_format: The format into which date columns will be converted to strings

    Returns:
        True if there are differences between the two dataframes, false otherwise
    """
    if not lambda_funcs:
        lambda_funcs = {}

    def apply_lambdas(df: pd.DataFrame) -> pd.DataFrame:
        if not df.empty:
            for col_name, lambda_func in lambda_funcs:
                df[col_name] = df.apply(lambda_func, axis=1)
        return df

    def convert_dates(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.select_dtypes(include=["datetime64"]).columns.tolist():
            df[col] = df[col].dt.strftime(date_format)
        return df

    def sort_df_cols(df: pd.DataFrame) -> pd.DataFrame:
        cols = df.columns.tolist()
        cols.sort()
        return df[cols]

    new_data_copy = (
        new_data.drop(["created_at", "updated_at"], axis=1, errors="ignore")
        .pipe(convert_dates)
        .replace(np.nan, "")
        .astype(str)
        .replace("[Nn]a[Tn]", "", regex=True)
        .astype(str)
        .apply(lambda x: x.astype(str).str.strip())
        .sort_values(by=sort_cols)
        .pipe(sort_df_cols)
        .reset_index(drop=True)
    )

    current_data_copy = (
        current_data.drop(["created_at", "updated_at"] + del_cols, axis=1, errors="ignore")
        .pipe(apply_lambdas)
        .pipe(convert_dates)
        .replace(np.nan, "")
        .astype(str)
        .replace("[Nn]a[Tn]", "", regex=True)
        .astype(str)
        .apply(lambda x: x.astype(str).str.strip())
        .sort_values(by=sort_cols)
        .pipe(sort_df_cols)
        .reset_index(drop=True)
    )

    return not new_data_copy.equals(current_data_copy)
