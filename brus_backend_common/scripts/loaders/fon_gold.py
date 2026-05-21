import argparse
import csv
import json
import datetime
import logging
import os

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


def main(
    local_file: os.PathLike = False, force_reload: bool = False, update_public_file: bool = True, metrics: dict = None
) -> None:
    """Load funding opportunity number lookup table.

    Args:
        local_file: path to local file to use instead of the FON Bronze data
        force_reload: whether or not to force a reload
        update_public_file: whether or not to update the public file
        metrics: dict of the metrics for the load
    """
    if not metrics:
        metrics = {}
    metrics["start_time"] = get_utc_now()

    fon_bronze = LAKEHOUSE_MODELS["bronze.funding_opportunity"]()
    fon_gold = LAKEHOUSE_MODELS["gold.funding_opportunity"]()

    if local_file:
        fon_bronze_data = pd.read_csv(local_file)
    else:
        fon_bronze_data = fon_bronze.to_pandas_df()

    fon_clean_data = clean_data(
        fon_bronze_data,
        field_map={
            "id": "internal_id",
            "number": "funding_opportunity_number",
            "title": "title",
            "agency": "agency_name",
            "oppstatus": "status",
            "opendate": "open_date",
            "closedate": "close_date",
            "doctype": "doc_type",
            "cfdalist": "assistance_listing_numbers",
        },
        field_options={},
    )

    diff_found = check_dataframe_diff(
        new_data=fon_clean_data,
        current_data=fon_gold.to_pandas_df(),
        del_cols=["funding_opportunity_id"],
        sort_cols=["internal_id"],
    )
    if force_reload or diff_found:
        logger.info("Differences found or reload forced, reloading table.")

        current_count = fon_gold.count()
        metrics["records_deleted"] = current_count

        fon_gold.save(fon_clean_data)

        new_count = len(fon_clean_data)
        logger.info(f"{new_count} records inserted to {fon_gold.TABLE_REF}")
        metrics["records_inserted"] = new_count

        if update_public_file:
            public_filename = "funding_opportunity_numbers.csv"
            logger.info(f"Saving to {public_filename}")
            fon_clean_data.to_csv(
                public_filename, index=False, quoting=csv.QUOTE_ALL, header=True, columns=["funding_opportunity_number"]
            )

            logger.info(f"Uploading {public_filename} to {CONFIG.PUBLIC_FILES_BUCKET}")
            s3 = _get_boto3("client", "s3")
            s3.upload_file(
                public_filename,
                CONFIG.PUBLIC_FILES_BUCKET,
                f"broker_reference_data/{public_filename}",
            )
            os.remove(public_filename)
    else:
        logger.info("No differences found, skipping reload.")

    metrics["end_time"] = get_utc_now()
    metrics["duration"] = metrics["end_time"] - metrics["start_time"]
    update_external_data_load_date(fon_gold, metrics["start_time"], metrics["end_time"])


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
    parser.add_argument(
        "--update_public_file",
        "-p",
        required=False,
        action="store_true",
        help="Update the public file as well",
    )
    return parser


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description="Populate FON Gold based on the FON Bronze data.")
    parser = setup_parser(parser)
    args = parser.parse_args()

    now = datetime.datetime.now()
    metrics = {
        "script_name": "load_fon_gold.py",
        "start_time": str(now),
        "records_deleted": 0,
        "records_inserted": 0,
    }

    main(args.local_file, args.force_reload, args.update_public_file, metrics=metrics)

    metrics_name = "fon_gold.json"
    logger.info(f"Saving metrics and upload to {CONFIG.METRICS_BUCKET}/{metrics_name}")
    with open(metrics_name, "w+") as metrics_file:
        json.dump(metrics, metrics_file, default=str)
    s3 = _get_boto3("client", "s3")
    s3.upload_file(metrics_name, CONFIG.METRICS_BUCKET, metrics_name)
