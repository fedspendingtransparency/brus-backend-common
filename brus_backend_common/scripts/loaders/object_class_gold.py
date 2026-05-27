import argparse
import json
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

    oc_bronze = LAKEHOUSE_MODELS["bronze.object_class"]()
    oc_gold = LAKEHOUSE_MODELS["gold.object_class"]()

    if local_file:
        oc_bronze_data = pd.read_csv(local_file, dtype=str)
    else:
        oc_bronze_data = oc_bronze.to_pandas_df()

    oc_clean_data = oc_bronze_data.pipe(
        clean_data,
        {"max_oc_code": "object_class_code", "max_object_class_name": "object_class_name"},
        {"object_class_code": {"pad_to_length": 3}},
    ).drop_duplicates(subset=["object_class_code"])

    diff_found = check_dataframe_diff(
        new_data=oc_clean_data,
        current_data=oc_gold.to_pandas_df(),
        del_cols=["object_class_id"],
        sort_cols=["object_class_code"],
    )
    if force_reload or diff_found:
        logger.info("Differences found or reload forced, reloading table.")

        current_count = oc_gold.count()
        metrics["records_deleted"] = current_count

        oc_gold.save(oc_clean_data)

        new_count = len(oc_clean_data)
        logger.info(f"{new_count} records inserted to {oc_gold.TABLE_REF}")
        metrics["records_inserted"] = new_count
    else:
        logger.info("No differences found, skipping reload.")

    metrics["end_time"] = get_utc_now()
    metrics["duration"] = str(metrics["end_time"] - metrics["start_time"])
    update_external_data_load_date(oc_gold, metrics["start_time"], metrics["end_time"])


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
    parser = argparse.ArgumentParser(description="Populate Object Class Gold based on the Object Class Bronze data.")
    parser = setup_parser(parser)
    args = parser.parse_args()

    metrics = {
        "script_name": "object_class_gold.py",
        "start_time": get_utc_now(),
        "records_deleted": 0,
        "records_inserted": 0,
    }

    main(args.local_file, args.force_reload, metrics=metrics)

    metrics_name = "object_class_gold.json"
    logger.info(f"Saving metrics and upload to {CONFIG.METRICS_BUCKET}/{metrics_name}")
    with open(metrics_name, "w+") as metrics_file:
        json.dump(metrics, metrics_file, default=str)
    s3 = _get_boto3("client", "s3")
    s3.upload_file(metrics_name, CONFIG.METRICS_BUCKET, metrics_name)
