import os
import pytest
from typing import List

from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.models import LAKEHOUSE_MODELS
from brus_backend_common.scripts.loaders import object_class_gold as oc_gold

TEST_BRONZE_OC_CSV = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "object_class.csv")


@pytest.fixture(scope="function")
def raw_pa_file():
    # Mimic placing the raw DEFC file in the expected location (directly or copied from another bucket)
    oc_bronze_model = LAKEHOUSE_MODELS["bronze.object_class"]()
    s3_client = _get_boto3("client", "s3")
    s3_client.upload_file(TEST_BRONZE_OC_CSV, oc_bronze_model.BUCKET_NAME, oc_bronze_model.RELATIVE_CSV_PATH)

    yield oc_bronze_model.RELATIVE_CSV_PATH

    s3_client.delete_object(Bucket=oc_bronze_model.BUCKET_NAME, Key=oc_bronze_model.RELATIVE_CSV_PATH)


def test_object_class(setup_teardown_buckets: List[str], external_data_load_dates: str, raw_pa_file: str):
    # Setup
    oc_bronze_model = LAKEHOUSE_MODELS["bronze.object_class"]()
    oc_bronze_model.initialize()

    oc_gold_model = LAKEHOUSE_MODELS["gold.object_class"]()
    oc_gold_model.initialize(recreate=True)

    # Run
    oc_gold.main(local_file=None, force_reload=False)

    # Check data
    assert oc_gold_model.exists()
    oc_gold_df = oc_gold_model.to_pandas_df()
    assert oc_gold_df is not None and not oc_gold_df.empty
    assert (
        oc_gold_df.loc[oc_gold_df["object_class_code"] == "258", "object_class_name"].values[0]
        == "Subsistence and support of persons"
    )

    # Get the time loaded to compare with later runs
    old_loaded_time = oc_gold_df.at[0, "created_at"]

    # Confirm the external load date was updated
    edld_model = LAKEHOUSE_MODELS["gold.external_data_load_date"]()

    assert edld_model.exists()
    edld_df = edld_model.to_pandas_df()
    assert edld_df is not None and not edld_df.empty
    assert not edld_df.loc[edld_df["name"] == "gold.object_class"].empty

    # Run it again directly with the original file to see if it skips
    oc_gold.main(local_file=TEST_BRONZE_OC_CSV, force_reload=False)
    new_loaded_time = oc_gold_model.to_pandas_df().at[0, "created_at"]
    assert old_loaded_time == new_loaded_time  # skipped

    # Run it again but force it
    oc_gold.main(local_file=None, force_reload=True)
    new_loaded_time = oc_gold_model.to_pandas_df().at[0, "created_at"]
    assert old_loaded_time != new_loaded_time  # not skipped
