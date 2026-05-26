import os
import pytest
from typing import List

from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.models import LAKEHOUSE_MODELS
from brus_backend_common.scripts.loaders import program_activity_gold as pa_gold

TEST_BRONZE_PA_CSV = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "program_activity.csv")


@pytest.fixture(scope="function")
def raw_pa_file():
    # Mimic placing the raw DEFC file in the expected location (directly or copied from another bucket)
    pa_bronze_model = LAKEHOUSE_MODELS["bronze.program_activity"]()
    s3_client = _get_boto3("client", "s3")
    s3_client.upload_file(TEST_BRONZE_PA_CSV, pa_bronze_model.BUCKET_NAME, pa_bronze_model.RELATIVE_CSV_PATH)

    yield pa_bronze_model.RELATIVE_CSV_PATH

    s3_client.delete_object(Bucket=pa_bronze_model.BUCKET_NAME, Key=pa_bronze_model.RELATIVE_CSV_PATH)


def test_program_activity(setup_teardown_buckets: List[str], external_data_load_dates: str, raw_pa_file: str):
    # Setup
    pa_bronze_model = LAKEHOUSE_MODELS["bronze.program_activity"]()
    pa_bronze_model.initialize()

    pa_gold_model = LAKEHOUSE_MODELS["gold.program_activity"]()
    pa_gold_model.initialize(recreate=True)

    # Run
    pa_gold.main(local_file=None, force_reload=False)

    # Check data
    assert pa_gold_model.exists()
    pa_gold_df = pa_gold_model.to_pandas_df()
    assert pa_gold_df is not None and not pa_gold_df.empty
    pa_filters = (
        (pa_gold_df["fiscal_year_period"] == "FY17P12")
        & (pa_gold_df["agency_id"] == "309")
        & (pa_gold_df["allocation_transfer_id"] == "012")
        & (pa_gold_df["account_number"] == "0200")
        & (pa_gold_df["program_activity_code"] == "0101")
    )
    assert pa_gold_df.loc[pa_filters, "program_activity_name"].values[0] == "appalachian development highway system"

    # Get the time loaded to compare with later runs
    old_loaded_time = pa_gold_df.at[0, "created_at"]

    # Confirm the external load date was updated
    edld_model = LAKEHOUSE_MODELS["gold.external_data_load_date"]()

    assert edld_model.exists()
    edld_df = edld_model.to_pandas_df()
    assert edld_df is not None and not edld_df.empty
    assert not edld_df.loc[edld_df["name"] == "gold.program_activity"].empty

    # Run it again directly with the original file to see if it skips
    pa_gold.main(local_file=TEST_BRONZE_PA_CSV, force_reload=False)
    new_loaded_time = pa_gold_model.to_pandas_df().at[0, "created_at"]
    assert old_loaded_time == new_loaded_time  # skipped

    # Run it again but force it
    pa_gold.main(local_file=None, force_reload=True)
    new_loaded_time = pa_gold_model.to_pandas_df().at[0, "created_at"]
    assert old_loaded_time != new_loaded_time  # not skipped
