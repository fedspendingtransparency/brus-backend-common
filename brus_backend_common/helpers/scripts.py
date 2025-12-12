import json
import logging
import requests
import time
import xmltodict
from typing import Any, Callable, Literal, Protocol

import numpy as np
import pandas as pd
from requests.exceptions import HTTPError, RequestException, ConnectionError, ReadTimeout
from urllib3.exceptions import ReadTimeoutError

from brus_backend_common.config import CONFIG
from brus_backend_common.helpers.generic import get_utc_now

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

""" A simple way of keeping track of the count when raising the Exception to the main script """
FAILURE_THRESHOLD_PERCENTAGE = 0.01


class FailureThresholdExceededError(Exception):
    def __init__(self, count):
        self.count = count


# TODO: Refacator to use the backoff library
def get_with_exception_hand(
    url_string: str,
    max_retries: int = 12,
    decode: bool = True,
    resp_type: Literal["json", "xml"] = "json",
    namespaces: dict | None = None,
    validate_response: Callable[[dict], bool] | None = None,
) -> dict[Any, Any] | requests.Response | None:
    """Retrieve data from a feed, allow for multiple retries, checks, and timeouts

    Args:
        url_string: string path to the feed we are getting data from
        max_retries: maximum number of retries to save time (range: 0-12)
        decode: whether to decode the response into a dict
        resp_type: the format of the response (either 'json' or 'xml')
        namespaces: dict of namespaces to clean up for the xml parsing
        validate_response: function to determine if the response is valid (ex. if the results are empty)

    Returns:
        The response from the url provided. Set decode to False for the raw response.

    Raises:
        ConnectionResetError, ReadTimeoutError, ConnectionError, ReadTimeout, HTTPError, RequestException:
            If there is a problem calling the url provided
        json.decoder.DecoderError, ExpatError:
            If there are issues decoding the response
        ValueError:
            If there's an error in the response or if there are no results when expected
    """
    if resp_type not in ["json", "xml"]:
        raise ValueError("resp_type must be 'xml' or 'json'.")
    if not 0 <= max_retries <= 12:
        raise ValueError("max_retries must be 0-12.")

    current_retries = 0
    retry_sleep_times = [0.5, 1, 5, 30, 60, 180, 300, 360, 420, 480, 540, 600]
    request_timeout = 60

    while current_retries < len(retry_sleep_times):
        resp = None
        try:
            resp = requests.get(url_string, timeout=request_timeout)
            resp.raise_for_status()

            if not decode:
                return_val = resp
            else:
                if resp_type == "xml":
                    resp_dict = xmltodict.parse(resp.text, process_namespaces=True, namespaces=namespaces)
                else:
                    resp_dict = resp.json()

                # Some feeds show their error in the response object
                resp_err = resp_dict.get("error")
                if resp_err:
                    message = resp_dict.get("message")
                    raise ValueError(f"Error processing response: {resp_err} {message}")

                if validate_response is not None and validate_response(resp_dict) is False:
                    raise ValueError(f"Invalid response provided: {resp_dict}")

                # type checker struggles with the concept of response, dict, or None
                return_val = resp_dict  # type: ignore
            break
        except (
            ConnectionResetError,
            ReadTimeoutError,
            ConnectionError,
            ReadTimeout,
            HTTPError,
            RequestException,
            json.decoder.JSONDecodeError,
            xmltodict.expat.ExpatError,
            ValueError,
        ) as e:
            if resp:
                logger.exception(resp.text)
            else:
                logger.exception(e)

            if current_retries < len(retry_sleep_times) and current_retries < max_retries:
                logger.info(
                    f"Sleeping {retry_sleep_times[current_retries]}s and then retrying"
                    f" with a max wait of {request_timeout}s..."
                )
                time.sleep(retry_sleep_times[current_retries])
                current_retries += 1
                request_timeout += 60
            else:
                logger.error("Maximum retry attempts exceeded.")
                raise e
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise e
    return return_val


def trim_nested_obj(obj):
    """A recursive version to trim all the values in a nested object

    Args:
        obj: object to recursively trim

    Returns:
        dict if object, list of values if list, trimmed if string, else obj
    """
    if isinstance(obj, dict):
        return {k: trim_nested_obj(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [trim_nested_obj(v) for v in obj]
    elif isinstance(obj, str):
        return obj.strip()
    return obj


def flatten_json(json_obj):
    """Flatten a JSON object into a single row.
        {'a': {'b': '1', 'c': ['d', 'e']}} => {'a_b': '1', 'a_c_1': 'd', 'a_c_2': 'e'}

    Args:
        json_obj: JSON object to flatten

    Returns:
        Single row of values from the json_obj JSON
    """
    out = {}

    def _flatten(list_item, name=""):
        if type(list_item) is dict:
            for item in list_item:
                _flatten(list_item[item], name + item + "_")
        elif type(list_item) is list:
            count = 0
            for item in list_item:
                _flatten(item, name + str(count) + "_")
                count += 1
        else:
            out[name[:-1]] = list_item

    _flatten(json_obj)
    return out


def exit_if_nonlocal(exit_code: int, blank_file: bool = False):
    """Simple reusable function for jobs with specific exit codes/messages

    Args:
        exit_code: the exit code to communicate to the box for a special case
        blank_file: whether to log that it was a blank file
    """
    if blank_file:
        logger.error("File was blank! Not loaded, routine aborted.")
    if not CONFIG.IS_LOCAL:
        raise SystemExit(exit_code)


class HasStr(Protocol):
    def __str__(self) -> str: ...


def clean_col_names(field: HasStr) -> str:
    """Define some data-munging functions that can be applied to pandas
    dataframes as necessary"""
    return str(field).lower().strip().replace(" ", "_").replace(",", "_")


def trim_item(item: Any) -> Any:
    if isinstance(item, str):
        return item.strip()
    return item


def pad_function(field: Any, pad_to: int, keep_null: bool) -> str | None:
    """Pads field to specified length."""
    if pd.isnull(field) or not str(field).strip():
        if keep_null:
            return None
        else:
            field = ""
    return str(field).strip().zfill(pad_to)


def clean_data(
    data: pd.DataFrame,
    field_map: dict[str, str],
    field_options: dict[str, dict[Literal["pad_to_length", "keep_null", "skip_duplicate", "strip_commas"], Any]],
    required_values: list | None = None,
    return_dropped_count: bool = False,
) -> pd.DataFrame | tuple[int, pd.DataFrame]:
    """Cleans up a dataframe that contains domain values.

    Args:
        data: dataframe of domain values
        field_map: dict that maps columns of the dataframe csv to our model columns
        field_options: dict with keys of attribute names, value contains a dict with options for that attribute.
            Current options are:
             "pad_to_length" which if present will pad the field with leading zeros up to
            specified length
            "keep_null" when set to true, empty fields will not be padded
            "skip_duplicate" which ignores subsequent lines that repeat values
            "strip_commas" which removes commas
        required_values: list of required values
        return_dropped_count: flag to return dropped count

    Returns:
        Dataframe conforming to requirements, and additionally the number of rows dropped for missing value
        if return_dropped_count argument is True

    Raises:
        FailureThresholdExceededError: If too many rows have been discarded during processing, fail the routine.
           Also fail if the file is blank.

    """
    # incoming .csvs often have extraneous blank rows at the end,
    # so get rid of those
    clean_df = data.copy(deep=True)
    clean_df.dropna(inplace=True, how="all")

    # clean the dataframe column names
    clean_df.rename(columns=clean_col_names, inplace=True)
    # make sure all values in fieldMap parameter are in the dataframe/csv file
    column_diff = set(field_map) - set(clean_df.columns)
    if column_diff:
        raise ValueError(f"The following fields are required per field_map: {column_diff}")
    # toss out any columns from the csv that aren't in the fieldMap parameter
    clean_df = clean_df[list(field_map.keys())]
    # rename columns as specified in fieldMap
    clean_df = clean_df.rename(columns=field_map)

    # trim all columns
    clean_df = clean_df.map(lambda x: trim_item(x) if len(str(x).strip()) else None)

    if required_values is None:
        required_values = []
    if len(required_values) > 0:
        # if file is blank, immediately fail
        if clean_df.empty or len(clean_df.shape) < 2:
            raise FailureThresholdExceededError(0)
        # check the columns that must have a valid value, and if they have white space,
        # replace with NaN so that dropna finds them.
        for value in required_values:
            clean_df[value].replace("", np.nan, inplace=True)
        # drop any rows that are missing required data
        cleaned = clean_df.dropna(subset=required_values)
        dropped = clean_df[np.invert(clean_df.index.isin(cleaned.index))]
        # log every dropped row
        for index, row in dropped.iterrows():
            logger.info(
                f"Dropped row due to faulty data: "
                f"fyq:{row['fiscal_year_period']}"
                f"--agency:{row['agency_id']}"
                f"--alloc:{row['allocation_transfer_id']}"
                f"--account:{row['account_number']}"
                f"--pa_code:{row['program_activity_code']}"
                f"--pa_name:{row['program_activity_name']}"
            )

        if (len(dropped.index) / len(clean_df.index)) > FAILURE_THRESHOLD_PERCENTAGE:
            raise FailureThresholdExceededError(len(dropped.index))
        logger.info(f"{len(dropped.index)} total rows dropped due to faulty data")
        clean_df = cleaned

    # apply column options as specified in fieldOptions param
    for col, options in field_options.items():
        if "pad_to_length" in options:
            # pad to specified length
            clean_df[col] = clean_df[col].apply(pad_function, args=(options["pad_to_length"], options.get("keep_null")))
        if options.get("strip_commas"):
            # remove commas for specified column
            # get rid of commas in dollar amounts
            clean_df[col] = clean_df[col].str.replace(",", "")

    # add created_at and updated_at columns
    now = get_utc_now()
    clean_df = clean_df.assign(created_at=now, updated_at=now)
    if return_dropped_count:
        return len(dropped.index), clean_df
    return clean_df
