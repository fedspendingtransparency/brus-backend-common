import os
import pytest
from typing import List

from botocore.exceptions import ClientError

from brus_backend_common.config import _SRC_ROOT_DIR, CONFIG
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.models import LAKEHOUSE_MODELS
from brus_backend_common.scripts.loaders import fon_gold

TEST_BRONZE_FON_CSV = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "test_fon_bronze.csv")


@pytest.fixture(scope="function")
def raw_fon_file():
    # Mimic placing the raw DEFC file in the expected location (directly or copied from another bucket)
    fon_bronze_model = LAKEHOUSE_MODELS["bronze.funding_opportunity"]()
    s3_client = _get_boto3("client", "s3")
    s3_client.upload_file(TEST_BRONZE_FON_CSV, fon_bronze_model.BUCKET_NAME, fon_bronze_model.RELATIVE_CSV_PATH)

    yield fon_bronze_model.RELATIVE_CSV_PATH

    s3_client.delete_object(Bucket=fon_bronze_model.BUCKET_NAME, Key=fon_bronze_model.RELATIVE_CSV_PATH)


def test_fon(setup_teardown_buckets: List[str], external_data_load_dates: str, raw_fon_file: str):
    # Setup
    fon_bronze_model = LAKEHOUSE_MODELS["bronze.funding_opportunity"]()
    fon_bronze_model.initialize()

    fon_gold_model = LAKEHOUSE_MODELS["gold.funding_opportunity"]()
    fon_gold_model.initialize(recreate=True)

    # Run
    fon_gold.main(local_file=None, force_reload=False, update_public_file=True)

    # Check data
    assert fon_gold_model.exists()
    fon_gold_df = fon_gold_model.to_pandas_df()
    assert fon_gold_df is not None and not fon_gold_df.empty
    assert fon_gold_df.loc[fon_gold_df["internal_id"] == 51463, "agency_name"].values[0] == "Test Agency 4"

    # Get the time loaded to compare with later runs
    old_loaded_time = fon_gold_df.at[0, "created_at"]

    # Confirm the external load date was updated
    edld_model = LAKEHOUSE_MODELS["gold.external_data_load_date"]()

    assert edld_model.exists()
    edld_df = edld_model.to_pandas_df()
    assert edld_df is not None and not edld_df.empty
    assert not edld_df.loc[edld_df["name"] == "gold.funding_opportunity"].empty

    # Confirm the public file got updated
    s3_client = _get_boto3("client", "s3")
    try:
        s3_client.head_object(
            Bucket=CONFIG.PUBLIC_FILES_BUCKET, Key="broker_reference_data/funding_opportunity_numbers.csv"
        )
        assert True
    except ClientError:
        assert False

    # Run it again directly with the original file to see if it skips
    fon_gold.main(local_file=TEST_BRONZE_FON_CSV, force_reload=False, update_public_file=False)
    new_loaded_time = fon_gold_model.to_pandas_df().at[0, "created_at"]
    assert old_loaded_time == new_loaded_time  # skipped

    # Run it again but force it
    fon_gold.main(local_file=None, force_reload=True, update_public_file=False)
    new_loaded_time = fon_gold_model.to_pandas_df().at[0, "created_at"]
    assert old_loaded_time != new_loaded_time  # not skipped
