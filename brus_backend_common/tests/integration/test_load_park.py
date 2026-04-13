import os

import pytest


import brus_backend_common.helpers.spark as spark_helper
from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.models.lakehouse_model import ExternalDataLoadDate
from brus_backend_common.models.reference import ProgramActivityPark
from brus_backend_common.scripts.loaders.load_park import ParkLoader


@pytest.fixture(scope="function")
def upload_park():

    pap_model = ProgramActivityPark()
    s3_client = _get_boto3("client", "s3")
    csv_file_path = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "PARK_PROGRAM_ACTIVITY.csv")
    s3_client.upload_file(csv_file_path, pap_model.BUCKET_NAME, ParkLoader.PARK_SUB_KEY + ParkLoader.PARK_FILE_NAME)

    yield pap_model.RELATIVE_TABLE_PATH

    s3_client.delete_object(Bucket=pap_model.BUCKET_NAME, Key=pap_model.RELATIVE_TABLE_PATH)


def test_load_park(upload_park):
    with spark_helper.SparkScriptSession() as spark:
        loader = ParkLoader(spark)
        loader.load_park_data(force_reload=True)

        pap_model = ProgramActivityPark(spark)
        assert pap_model.exists()
        df = pap_model.to_pandas_df()
        assert df is not None and not df.empty
        assert (
            df.loc[df["park_code"] == "5ZBPXDKGVPJ", "park_name"].values[0]
            == "Compensation of Members, Senate (Direct)"
        )

        edld_model = ExternalDataLoadDate(spark)
        assert edld_model.exists()
        df = edld_model.to_pandas_df()
        assert df is not None and not df.empty
        assert not df.loc[df["name"] == pap_model.TABLE_REF].empty
