import pandas as pd
from datetime import date

from brus_backend_common.helpers.generic import get_utc_now
from brus_backend_common.helpers import pandas


def test_check_dataframe_diff():
    now = get_utc_now()
    current_df = pd.DataFrame(
        {
            "name": ["Tom", "Nick", "Krish", "Jack"],
            "age": ["20", "21", "19", "18"],
            "amount": ["1,000", "2,000", "3,000", "4,000"],
            "date": [date(2026, 3, 25), date(2026, 3, 26), date(2026, 3, 27), date(2026, 3, 28)],
            "created_at": [now] * 4,
            "updated_at": [now] * 4,
        }
    )
    del_cols = ["age"]
    sort_cols = ["date"]

    def strip_commas(row: pd.Series) -> pd.Series:
        return row["amount"].replace(",", "")

    lambda_funcs = [("amount", strip_commas)]

    date_format = "%Y%m%d"

    # Check there's no new data
    # Ignoring age column
    # Stripping commas from amount column
    # Converting dates
    new_df = pd.DataFrame(
        {
            "name": ["Tom", "Nick", "Krish", "Jack"],
            "amount": ["1000", "2000", "3000", "4000"],
            "date": [date(2026, 3, 25), date(2026, 3, 26), date(2026, 3, 27), date(2026, 3, 28)],
            "created_at": [now] * 4,
            "updated_at": [now] * 4,
        }
    )
    assert (
        pandas.check_dataframe_diff(
            new_data=new_df,
            current_data=current_df,
            del_cols=del_cols,
            sort_cols=sort_cols,
            lambda_funcs=lambda_funcs,
            date_format=date_format,
        )
        is False
    )

    # Check there *is* new data
    new_row = pd.DataFrame(
        {
            "name": ["Sally "],
            "amount": [" 5000"],
            "date": [date(2026, 3, 29)],
            "created_at": [now],
            "updated_at": [now],
        }
    )
    new_df = pd.concat([new_df, new_row], ignore_index=True)
    assert (
        pandas.check_dataframe_diff(
            new_data=new_df,
            current_data=current_df,
            del_cols=del_cols,
            sort_cols=sort_cols,
            lambda_funcs=lambda_funcs,
            date_format=date_format,
        )
        is True
    )
