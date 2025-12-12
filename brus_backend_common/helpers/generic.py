import calendar
import logging
import os
import re
from datetime import date, datetime, UTC
from dateutil.parser import parse
from pathlib import Path
from typing import Any, Generator, Sequence
from zipfile import ZipFile, ZIP_DEFLATED

import pandas as pd
import requests
from urllib3.exceptions import ReadTimeoutError

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

RETRY_REQUEST_EXCEPTIONS = (
    requests.exceptions.RequestException,
    ConnectionError,
    ConnectionResetError,
    ReadTimeoutError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ChunkedEncodingError,
)


def fy(raw_date: date | datetime | str | None) -> int | None:
    """Get fiscal year from date, datetime, or date string

    Args:
        raw_date: date to be parsed

    Returns:
        integer representing the fiscal year associated with the date
    """

    if raw_date is None:
        return None

    if isinstance(raw_date, str):
        try:
            raw_date = parse(raw_date)
        except Exception:
            raise TypeError(f"{raw_date} needs to be a valid date/datetime string")
    elif not isinstance(raw_date, (date, datetime)):
        raise TypeError(f"{raw_date} needs to be a valid date/datetime")

    result = raw_date.year
    if raw_date.month > 9:
        result += 1

    return result


def fy_period_calendar_dates(year: int, period: int) -> tuple[date, date]:
    """Converts a FY period to the real-life start and end dates they represents.

    Args:
        year: integer representing the year to use
        period: integer representing the period (month of the fiscal year) to use

    Returns:
        Date objects of the start and end dates of the given period
    """
    # Make sure year is in the proper format
    if not year or not re.match(r"^\d{4}$", str(year)):
        raise ValueError("Year must be in YYYY format.")
    # Make sure period is a number 2-12
    if not period or period not in list(range(2, 13)):
        raise ValueError("Period must be an integer 2-12.")

    # Set the actual month, add 12 if it's negative so it loops around and adjusts the year
    month = period - 3
    if month < 1:
        month += 12
        year -= 1

    # Get the last day of the month
    last_day_of_month = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day_of_month)


def format_internal_tas(row: pd.Series | dict[str, str]) -> str:
    """Concatenate TAS components into a single field for internal use.

    Args:
        row: row of data with TAS elements

    Returns:
        TAS components concatenated into a single string
    """
    # This formatting should match the Broker's internal TAS string
    tas_components = {
        "allocation_transfer_agency": "000",
        "agency_identifier": "000",
        "beginning_period_of_availa": "0000",
        "ending_period_of_availabil": "0000",
        "availability_type_code": " ",
        "main_account_code": "0000",
        "sub_account_code": "000",
    }
    return "".join(
        [
            (
                row[component].strip()
                if component in row and isinstance(row[component], str) and row[component].strip()
                else default
            )
            for component, default in tas_components.items()
        ]
    )


def deep_merge(left: dict, right: dict):
    """Deep merge dictionaries, replacing values from right"""
    if isinstance(left, dict) and isinstance(right, dict):
        result = left.copy()
        for key in right:
            if key in left:
                result[key] = deep_merge(left[key], right[key])
            else:
                result[key] = right[key]
        return result
    else:
        return right


def batch(iterable: Sequence, n: int = 1) -> Generator[Any, None, None]:
    """Simple function to create batches from a list

    Args:
        iterable: the list to be batched
        n: the size of the batches

    Yields:
        the same list (iterable) in batches depending on the size of N
    """
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx : min(ndx + n, length)]


def step(iterable: Sequence, start: int, direction: int, include_start=True) -> Generator[tuple[int, Any], None, None]:
    """Simple function to step through list in a direction

    Args:
        iterable: the list to be batched
        start: the starting point in the list, 0-indexed, must be greater or equal to 0
        direction: the direction to step in the list, 0 returns empty list
        include_start: whether or not to include start as a step

    Yields:
        list (iterable) of forward or backward steps to move including indexes similar to enumerate (index, value)
    """
    if start < 0 or start >= len(iterable):
        raise IndexError("Start must be in range of the 0-indexed list.")

    sign = -1 if direction < 0 else 1
    if not include_start:
        start += sign
    true_start = None if start < 0 else start
    end = (true_start or 0) + direction if (true_start or 0) + direction >= 0 else None

    if not (true_start is None and end is None):
        i = true_start or 0
        for v in iterable[true_start:end:sign]:
            yield i, v
            i += sign


def zip_dir(dir_path: str | os.PathLike, name: str) -> os.PathLike:
    """Simply zips the contents of a directory (currently only supports zip)

    Args:
        dir_path: the path of the directory to be zipped
        name: name of the zip

    Returns:
        path to zip generated
    """
    zip_path = Path(os.path.join(os.path.abspath(os.path.join(dir_path, os.pardir)), f"{name}.zip"))
    with ZipFile(zip_path, "w", ZIP_DEFLATED) as zip_h:
        for root, dirs, files in os.walk(str(dir_path)):
            for file in files:
                zip_h.write(
                    os.path.join(root, file),
                    os.path.relpath(os.path.join(root, file), os.path.join(dir_path, "..")),
                )
    return zip_path


def get_timestamp() -> str:
    """Gets a timestamp in seconds

    Returns:
        a string representing seconds since the epoch
    """
    return str(int(get_utc_now().timestamp()))


def get_utc_now() -> datetime:
    """Gets the current time in UTC

    Returns:
        a datetime with no timezone info that reflects the current time in UTC
    """
    return datetime.now(UTC).replace(tzinfo=None)
