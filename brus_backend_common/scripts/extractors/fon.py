import argparse
import datetime
import json
import logging
import pandas as pd
import requests

from brus_backend_common.config import CONFIG
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.helpers.generic import get_utc_now
from brus_backend_common.logging import configure_logging
from brus_backend_common.models import LAKEHOUSE_MODELS
from brus_backend_common.models.lakehouse_model import update_external_data_load_date


logger = logging.getLogger(__name__)
FON_URL = "https://apply07.grants.gov/grantsws/rest/opportunities/search/"
BATCH_SIZE = 10000


def extract_fon_data() -> pd.DataFrame:
    logger.info("Pulling FON data")

    post_body = {"startRecordNum": 0, "oppStatuses": "forecasted|posted|closed|archived", "rows": BATCH_SIZE}
    fon_resp = requests.post(FON_URL, json=post_body).json()
    total_records = fon_resp["hitCount"]
    fon_list = fon_resp["oppHits"]

    while post_body["startRecordNum"] + BATCH_SIZE < total_records:
        post_body["startRecordNum"] += BATCH_SIZE
        fon_resp = requests.post(FON_URL, json=post_body).json()
        fon_list += fon_resp["oppHits"]

    logger.info("Pulled FON data")

    return pd.DataFrame(fon_list)


def main(metrics: dict = None):
    if not metrics:
        metrics = {}
    metrics["start_time"] = get_utc_now()

    fon_df = extract_fon_data()

    local_fon_csv = "fon.csv"
    logger.info(f"Saving to {local_fon_csv}")
    fon_df.to_csv(local_fon_csv, index=False)

    fon_bronze = LAKEHOUSE_MODELS["bronze.funding_opportunity"]()
    logger.info(f"Uploading to {fon_bronze.CSV_PATH}")
    s3 = _get_boto3("client", "s3")
    s3.upload_file(local_fon_csv, fon_bronze.BUCKET_NAME, fon_bronze.RELATIVE_CSV_PATH)

    metrics["end_time"] = get_utc_now()
    metrics["duration"] = str(metrics["end_time"] - metrics["start_time"])
    update_external_data_load_date(fon_bronze, metrics["start_time"], metrics["end_time"])


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description="Pull FON data from Grants.gov and save it into the bronze bucket.")

    now = datetime.datetime.now()
    metrics = {
        "script_name": "fon.py",
    }

    main(metrics=metrics)

    metrics_name = "extract_fon.json"
    logger.info(f"Saving methrics and upload to {CONFIG.METRICS_BUCKET}/{metrics_name}")
    with open(metrics_name, "w+") as metrics_file:
        json.dump(metrics, metrics_file)
    s3 = _get_boto3("client", "s3")
    s3.upload_file(metrics_name, CONFIG.METRICS_BUCKET, metrics_name)
