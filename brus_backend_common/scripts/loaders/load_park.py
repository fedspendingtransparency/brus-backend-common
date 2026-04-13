import argparse
import datetime
import io
import json
import logging
from enum import Enum

import pandas as pd

from brus_backend_common.config import CONFIG
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.helpers.scripts import (
    clean_data,
    exit_if_nonlocal,
)
import brus_backend_common.helpers.spark as spark_helper
from brus_backend_common.models.lakehouse_model import ExternalDataLoadDate, update_external_data_load_date
from brus_backend_common.models.reference import ProgramActivityPark


logger = logging.getLogger(__name__)


class ParkLoader:

    class ErrorCodes(Enum):
        EMPTY_DATA = 4
        SKIPPED = 6

    PARK_BUCKET = CONFIG.DATA_SOURCES_BUCKET
    PARK_SUB_KEY = "OMB_Data/"
    PARK_FILE_NAME = "PARK_PROGRAM_ACTIVITY.csv"

    def __init__(self, spark):
        self.spark = spark

    def get_park_df(self) -> pd.DataFrame:
        logger.info("Getting the PARK file")
        s3 = _get_boto3("client", "s3")
        response = s3.get_object(Bucket=self.PARK_BUCKET, Key=self.PARK_SUB_KEY + self.PARK_FILE_NAME)
        pa_file = io.BytesIO(response["Body"].read())
        raw_data = pd.read_csv(pa_file, dtype=str, na_filter=False)
        return clean_data(
            raw_data,
            {
                "fy": "fiscal_year",
                "pd": "period",
                "alloc_xfer_agency": "allocation_transfer_id",
                "aid": "agency_id",
                "main_acct": "main_account_number",
                "sub_acct": "sub_account_number",
                "park": "park_code",
                "park_name": "park_name",
            },
            {
                "agency_id": {"pad_to_length": 3},
                "allocation_transfer_id": {"pad_to_length": 3, "keep_null": True},
                "main_account_number": {"pad_to_length": 4},
                "sub_account_number": {"pad_to_length": 3, "keep_null": True},
            },
        )

    def get_date_of_current_park_upload(self) -> datetime.datetime:
        last_uploaded = _get_boto3("client", "s3").head_object(
            Bucket=self.PARK_BUCKET, Key=self.PARK_SUB_KEY + self.PARK_FILE_NAME
        )["LastModified"]
        # LastModified is coming back to us in UTC already; just drop the TZ.
        last_uploaded = last_uploaded.replace(tzinfo=None)
        return last_uploaded

    def get_stored_park_last_upload(self) -> datetime.datetime | None:
        edld = ExternalDataLoadDate(self.spark)
        if not edld.exists():
            edld.initialize(recreate=True)
        df = edld.to_pandas_df()
        last_stored_obj = df[df.name == ProgramActivityPark(self.spark).TABLE_REF]
        return (
            None
            if last_stored_obj.empty
            else datetime.datetime.strptime(last_stored_obj.last_load_date_start.values[0], "%Y-%m-%d %H:%M:%S.%f")
        )

    @property
    def is_skipped(self) -> bool:
        logger.info("Checking PARK upload dates to see if we can skip.")
        last_upload = self.get_date_of_current_park_upload()
        stored_upload = self.get_stored_park_last_upload()
        skipped = False
        if stored_upload and not (last_upload > stored_upload):
            logger.info("Skipping load as it's already been done")
            skipped = True
        return skipped

    @staticmethod
    def export_public_park(raw_data: pd.DataFrame, export_name: str = "park.csv") -> None:
        logger.info("Exporting loaded PARK file to {}".format(export_name))
        raw_data.to_csv(export_name, index=0)

    def load_park_data(
        self,
        force_reload: bool = False,
        export: bool = False,
    ) -> int | None:
        start_time = datetime.datetime.now()
        metrics_json = {
            "script_name": "load_park.py",
            "start_time": str(start_time),
            "records_deleted": 0,
            "records_inserted": 0,
        }
        skipped = False if force_reload else self.is_skipped
        if not skipped:
            try:
                df = self.get_park_df()
            except pd.errors.EmptyDataError:
                return self.ErrorCodes.EMPTY_DATA.value
            if export:
                self.export_public_park(df)
            pap = ProgramActivityPark(self.spark)
            if not pap.exists():
                pap.initialize(recreate=True)
            metrics_json["records_deleted"] = pap.count()
            pap.save(df)
            end_time = datetime.datetime.now()
            update_external_data_load_date(pap, start_time, end_time)
            num_records = pap.count()
            logger.info("{} records inserted to {}".format(num_records, pap.TABLE_REF))
            metrics_json["records_inserted"] = num_records
            metrics_json["duration"] = str(end_time - start_time)
        with open("load_park_metrics.json", "w+") as metrics_file:
            json.dump(metrics_json, metrics_file)
        if skipped:
            return self.ErrorCodes.SKIPPED.value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Loads in Program Activity data")
    parser.add_argument(
        "-e", "--export", help="If provided, exports a public version of the file locally", action="store_true"
    )
    parser.add_argument("-f", "--force", help="If provided, forces a reload", action="store_true")
    args = parser.parse_args()
    with spark_helper.SparkScriptSession() as spark:
        loader = ParkLoader(spark)
        exit_code = loader.load_park_data(force_reload=args.force, export=args.export)
    if exit_code is not None:
        exit_if_nonlocal(exit_code)
