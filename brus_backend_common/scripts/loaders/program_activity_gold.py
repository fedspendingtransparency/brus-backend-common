import argparse
import json
import logging
import os
import re

import numpy as np
import pandas as pd

from brus_backend_common.config import CONFIG
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.helpers.generic import get_utc_now
from brus_backend_common.helpers.scripts import clean_data
from brus_backend_common.helpers.pandas import check_dataframe_diff
from brus_backend_common.logging import configure_logging
from brus_backend_common.models import LAKEHOUSE_MODELS
from brus_backend_common.models.lakehouse_model import update_external_data_load_date


logger = logging.getLogger(__name__)


def lowercase_or_notify(x):
    """Lowercases the input if it is valid, otherwise logs the error and sets a default value

    Args:
        String to lowercase

    Returns:
        Lowercased string if possible, else unmodified string or default value.
    """
    try:
        return x.lower()
    except Exception:
        if x and not np.isnan(x):
            logger.info("Program activity of {} was unable to be lowercased. Entered as-is.".format(x))
            return x
        else:
            logger.info("Null value found for program activity name. Entered default value.")  # should not happen
            return "(not provided)"


def convert_fyq_to_fyp(fyq):
    """Converts the fyq provided to fyp if it is in fyq format. Do nothing if it is already in fyp format

    Args:
        fyq: String to convert or leave alone fiscal year quarters

    Returns:
        FYQ converted to FYP or left the same
    """
    # If it's in quarter format, convert to period
    if re.match(r"^FY\d{2}Q\d$", str(fyq).upper().strip()):
        # Make sure it's all uppercase and replace the Q with a P
        fyq = fyq.upper().strip().replace("Q", "P")
        # take the last character in the string (the quarter), multiply by 3, replace
        quarter = fyq[-1]
        period = str(int(quarter) * 3).zfill(2)
        fyq = fyq[:-1] + period
        return fyq
    return fyq


def main(local_file: os.PathLike = False, force_reload: bool = False, metrics: dict = None) -> None:
    """Load funding opportunity number lookup table.

    Args:
        local_file: path to local file to use instead of the FON Bronze data
        force_reload: whether or not to force a reload
        metrics: dict of the metrics for the load
    """
    if not metrics:
        metrics = {}
    metrics["start_time"] = get_utc_now()

    pa_bronze = LAKEHOUSE_MODELS["bronze.program_activity"]()
    pa_gold = LAKEHOUSE_MODELS["gold.program_activity"]()

    if local_file:
        pa_bronze_data = pd.read_csv(local_file, dtype=str, na_filter=False)
    else:
        pa_bronze_data = pa_bronze.to_pandas_df(na_filter=False)

    pa_clean_data = (
        pa_bronze_data.pipe(
            clean_data,
            {
                "reporting_period": "fiscal_year_period",
                "agency_identifier_code": "agency_id",
                "allocation_transfer_agency_identifier_code": "allocation_transfer_id",
                "main_account_code": "account_number",
                "program_activity_code": "program_activity_code",
                "program_activity_name": "program_activity_name",
            },
            {
                "program_activity_code": {"pad_to_length": 4},
                "agency_id": {"pad_to_length": 3},
                "allocation_transfer_id": {"pad_to_length": 3, "keep_null": True},
                "account_number": {"pad_to_length": 4},
            },
            ["agency_id", "program_activity_code", "account_number", "program_activity_name"],
        )
        # Lowercase Program Activity Name
        .assign(program_activity_name=lambda r: r["program_activity_name"].apply(lambda v: lowercase_or_notify(v)))
        # Convert FYQ to FYP
        .assign(fiscal_year_period=lambda r: r["fiscal_year_period"].apply(lambda v: convert_fyq_to_fyp(v)))
        # because we're only loading a subset of program activity info, there will be duplicate records in the
        # dataframe. this is ok, but need to de-duped before the lakehouse load.
        .drop_duplicates()
    )

    diff_found = check_dataframe_diff(
        new_data=pa_clean_data,
        current_data=pa_gold.to_pandas_df(na_filter=False),
        del_cols=["program_activity_id"],
        sort_cols=[
            "fiscal_year_period",
            "agency_id",
            "allocation_transfer_id",
            "account_number",
            "program_activity_code",
            "program_activity_name",
        ],
    )
    if force_reload or diff_found:
        logger.info("Differences found or reload forced, reloading table.")

        current_count = pa_gold.count()
        metrics["records_deleted"] = current_count

        pa_gold.save(pa_clean_data)

        new_count = len(pa_clean_data)
        logger.info(f"{new_count} records inserted to {pa_gold.TABLE_REF}")
        metrics["records_inserted"] = new_count
    else:
        logger.info("No differences found, skipping reload.")

    metrics["end_time"] = get_utc_now()
    metrics["duration"] = str(metrics["end_time"] - metrics["start_time"])
    update_external_data_load_date(pa_gold, metrics["start_time"], metrics["end_time"])


def setup_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Separating parser functionality as USAS uses Django Commands"""
    parser.add_argument(
        "--local_file",
        "-l",
        type=str,
        required=False,
        help="Load from a local file instead of pulling from S3",
    )
    parser.add_argument(
        "--force_reload",
        "-f",
        required=False,
        action="store_true",
        help="Force reload of the data",
    )
    return parser


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(
        description="Populate Program Activity Gold based on the Program Activity Bronze data."
    )
    parser = setup_parser(parser)
    args = parser.parse_args()

    metrics = {
        "script_name": "program_activity_gold.py",
        "start_time": get_utc_now(),
        "records_deleted": 0,
        "records_inserted": 0,
    }

    main(args.local_file, args.force_reload, metrics=metrics)

    metrics_name = "program_activity_gold.json"
    logger.info(f"Saving metrics and upload to {CONFIG.METRICS_BUCKET}/{metrics_name}")
    with open(metrics_name, "w+") as metrics_file:
        json.dump(metrics, metrics_file, default=str)
    s3 = _get_boto3("client", "s3")
    s3.upload_file(metrics_name, CONFIG.METRICS_BUCKET, metrics_name)
