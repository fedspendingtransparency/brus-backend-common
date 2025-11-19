import json
import logging
import requests
import sys
import time
import xmltodict
from requests.exceptions import ConnectionError, ReadTimeout
from urllib3.exceptions import ReadTimeoutError

from brus_backend_common.config import CONFIG
from brus_backend_common.models import ExternalDataLoadDateDelta

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

""" A simple way of keeping track of the count when raising the Exception to the main script """


class FailureThresholdExceededError(Exception):
    def __init__(self, count):
        self.count = count


def list_data(data):
    """Make dictionaries into a list

    Args:
        data: dictionaries to turn into a list of those dictionaries

    Returns:
        a list of dictionaries or the data that was provided if it wasn't a dictionary
    """
    if isinstance(data, dict):
        # make a list so it's consistent
        data = [
            data,
        ]
    return data


def get_xml_with_exception_hand(url_string, namespaces, expect_entries=True):
    """Retrieve XML data from a feed, allow for multiple retries and timeouts

    Args:
        url_string: string path to the feed we are getting data from
        namespaces: dict of namespaces to clean up for the xml parsing
        expect_entries: boolean of whether we should check the length of the list

    Returns:
        The XML response from the url provided

    Raises:
        ConnectionResetError, ReadTimeoutError, ConnectionError, ReadTimeout:
            If there is a problem calling the url provided
        KeyError:
            If one of the expected keys doesn't exist
    """
    exception_retries = -1
    retry_sleep_times = [5, 30, 60, 180, 300, 360, 420, 480, 540, 600]
    request_timeout = 60

    while exception_retries < len(retry_sleep_times):
        try:
            resp = requests.get(url_string, timeout=request_timeout)
            if expect_entries:
                # we should always expect entries, otherwise we shouldn't be calling it
                resp_dict = xmltodict.parse(resp.text, process_namespaces=True, namespaces=namespaces)
                len(list_data(resp_dict["feed"]["entry"]))
            break
        except (
            ConnectionResetError,
            ReadTimeoutError,
            ConnectionError,
            ReadTimeout,
            KeyError,
        ) as e:
            exception_retries += 1
            request_timeout += 60
            if exception_retries < len(retry_sleep_times):
                logger.info(
                    "Connection exception. Sleeping {}s and then retrying with a max wait of {}s...".format(
                        retry_sleep_times[exception_retries], request_timeout
                    )
                )
                time.sleep(retry_sleep_times[exception_retries])
            else:
                logger.info("Connection to feed lost, maximum retry attempts exceeded.")
                raise e
    return resp


# TODO: Refacator to use backoff
def get_with_exception_hand(url_string):
    """Retrieve data from API, allow for multiple retries and timeouts

    Args:
        url_string: URL to make the request to

    Returns:
        API response from the URL
    """
    exception_retries = -1
    retry_sleep_times = [5, 30, 60, 180, 300, 360, 420, 480, 540, 600]
    request_timeout = 60
    response_dict = None

    def handle_resp(exception_retries, request_timeout):
        exception_retries += 1
        request_timeout += 60
        if exception_retries < len(retry_sleep_times):
            logger.info(
                "Sleeping {}s and then retrying with a max wait of {}s...".format(
                    retry_sleep_times[exception_retries], request_timeout
                )
            )
            time.sleep(retry_sleep_times[exception_retries])
            return exception_retries, request_timeout
        else:
            logger.error("Maximum retry attempts exceeded.")
            sys.exit(2)

    while exception_retries < len(retry_sleep_times):
        # Adding this to log the response if we're unable to decode it
        resp = None
        try:
            resp = requests.get(url_string, timeout=request_timeout)
            response_dict = json.loads(resp.text)
            # We get errors back as regular JSON, need to catch them somewhere
            if response_dict.get("error"):
                err = response_dict.get("error")
                message = response_dict.get("message")
                logger.warning("Error processing response: {} {}".format(err, message))
                exception_retries, request_timeout = handle_resp(exception_retries, request_timeout)
                continue
            break
        except (
            ConnectionResetError,
            ReadTimeoutError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
            json.decoder.JSONDecodeError,
        ) as e:
            if resp:
                logger.exception(resp.text)
            logger.exception(e)
            exception_retries, request_timeout = handle_resp(exception_retries, request_timeout)

    return response_dict


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


def update_external_data_load_date(spark, data_type, start_time, end_time):
    """Update the external_data_load_date table with the start and end times for the given data type

    Args:
        spark: current spark connection
        data_type: a string indicating the data type of the external data load
        start_time: a datetime object indicating the start time of the external data load
        end_time: a datetime object indicating the end time of the external data load
    """
    df = ExternalDataLoadDateDelta(spark).to_pandas_df()
    last_stored_obj = df[df.name == data_type]
    if last_stored_obj.empty:
        raise ValueError("Data type not found in external data load date table/csv. Please update it beforehand.")
    last_stored_obj.last_load_date_start = start_time
    last_stored_obj.last_load_date_end = end_time
    df.merge(last_stored_obj)


def log_blank_file():
    """Helper function for specific reused log message"""
    logger.error("File was blank! Not loaded, routine aborted.")


def exit_if_nonlocal(exit_code):
    if not CONFIG.IS_LOCAL:
        sys.exit(exit_code)
