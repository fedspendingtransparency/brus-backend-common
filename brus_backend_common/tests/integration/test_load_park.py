import os
from unittest.mock import patch

import pytest


import brus_backend_common.helpers.spark as spark_helper
from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.models.lakehouse_model import ExternalDataLoadDate
from brus_backend_common.models.reference import ProgramActivityParkBronze, ProgramActivityParkGold
from brus_backend_common.scripts.loaders.load_park import ParkLoader
from brus_backend_common.tests.conftest_spark import s3_unittest_data_bucket


@pytest.fixture(scope="function")
def upload_park(s3_unittest_data_bucket):
    bronze = ProgramActivityParkBronze()
    s3_client = _get_boto3("client", "s3")
    csv_file_path = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "PARK_PROGRAM_ACTIVITY.csv")
    s3_client.upload_file(csv_file_path, s3_unittest_data_bucket, bronze.RELATIVE_CSV_PATH)

    yield

    s3_client.delete_object(Bucket=bronze.BUCKET_NAME, Key=bronze.RELATIVE_CSV_PATH)


def test_load_park(s3_unittest_data_bucket, hive_unittest_metastore_db, upload_park):
    with (
        patch.object(ProgramActivityParkBronze, "BUCKET_NAME", s3_unittest_data_bucket),
        patch.object(ProgramActivityParkGold, "BUCKET_NAME", s3_unittest_data_bucket),
    ):
        pap_model = ProgramActivityParkGold()
        assert not pap_model.exists()

        loader = ParkLoader()
        loader.load_park_data(force_reload=True)

        pap_model = ProgramActivityParkGold()
        assert pap_model.exists()
        df = pap_model.to_pandas_df()
        assert df is not None and not df.empty
        assert (
            df.loc[df["park_code"] == "5ZBPXDKGVPJ", "park_name"].values[0]
            == "Compensation of Members, Senate (Direct)"
        )

        edld_model = ExternalDataLoadDate()
        assert edld_model.exists()
        df = edld_model.to_pandas_df()
        assert df is not None and not df.empty
        assert not df.loc[df["name"] == pap_model.TABLE_REF].empty
