import os
from zipfile import ZipFile, ZIP_DEFLATED
import re
import calendar
import logging
import requests
from requests.packages.urllib3.exceptions import ReadTimeoutError
from dateutil.parser import parse
from datetime import date, datetime, UTC

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


def year_period_to_dates(year, period):
    """Converts a year and period to the real-life start and end dates they represents.

    Args:
        year: integer representing the year to use
        period: integer representing the period (month of the fiscal year) to use

    Returns:
        Strings representing the start and end dates of the given quarter
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

    start = str(month).zfill(2) + "/01/" + str(year)
    end = str(month).zfill(2) + "/" + str(last_day_of_month) + "/" + str(year)

    return start, end


def format_internal_tas(row):
    """Concatenate TAS components into a single field for internal use.

    Args:
        row: row of data with TAS elements

    Returns:
        TAS components concatenated into a single string
    """
    # This formatting should match formatting in dataactcore.models.stagingModels concat_tas
    ata = (
        row["allocation_transfer_agency"].strip()
        if row["allocation_transfer_agency"] and row["allocation_transfer_agency"].strip()
        else "000"
    )
    aid = row["agency_identifier"].strip() if row["agency_identifier"] and row["agency_identifier"].strip() else "000"
    bpoa = (
        row["beginning_period_of_availa"].strip()
        if row["beginning_period_of_availa"] and row["beginning_period_of_availa"].strip()
        else "0000"
    )
    epoa = (
        row["ending_period_of_availabil"].strip()
        if row["ending_period_of_availabil"] and row["ending_period_of_availabil"].strip()
        else "0000"
    )
    atc = (
        row["availability_type_code"].strip()
        if row["availability_type_code"] and row["availability_type_code"].strip()
        else " "
    )
    mac = row["main_account_code"].strip() if row["main_account_code"] and row["main_account_code"].strip() else "0000"
    sac = row["sub_account_code"].strip() if row["sub_account_code"] and row["sub_account_code"].strip() else "000"
    return "".join([ata, aid, bpoa, epoa, atc, mac, sac])


def fy(raw_date):
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
            raise TypeError("{} needs to be a valid date/datetime string".format(raw_date))
    elif not isinstance(raw_date, (date, datetime)):
        raise TypeError("{} needs to be a valid date/datetime".format(raw_date))

    result = raw_date.year
    if raw_date.month > 9:
        result += 1

    return result


def batch(iterable, n=1):
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


def zip_dir(dir_path, name):
    """Simply zips the contents of a directory (currently only supports zip)

    Args:
        dir_path: the path of the directory to be zipped
        name: name of the zip

    Returns:
        path to zip generated
    """
    zip_path = os.path.join(os.path.abspath(os.path.join(dir_path, os.pardir)), "{}.zip".format(name))
    zip_h = ZipFile(zip_path, "w", ZIP_DEFLATED)
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            zip_h.write(
                os.path.join(root, file),
                os.path.relpath(os.path.join(root, file), os.path.join(dir_path, "..")),
            )
    zip_h.close()
    return zip_path


def get_timestamp():
    """Gets a timestamp in seconds

    Returns:
        a string representing seconds since the epoch
    """
    return str(int((get_utc_now() - datetime(1970, 1, 1)).total_seconds()))


def get_utc_now():
    """Gets the current time in UTC

    Returns:
        a datetime with no timezone info that reflects the current time in UTC
    """
    return datetime.now(UTC).replace(tzinfo=None)
