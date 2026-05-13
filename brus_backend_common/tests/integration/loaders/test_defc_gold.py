import os
import pytest
from typing import List

from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.scripts.loaders import defc_gold
from brus_backend_common.models import LAKEHOUSE_MODELS


@pytest.fixture(scope="function")
def raw_defc_file():
    # Mimic placing the raw DEFC file in the expected location (directly or copied from another bucket)
    defc_model = LAKEHOUSE_MODELS["bronze.defc"]()
    s3_client = _get_boto3("client", "s3")
    csv_file_path = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "DEFC_LIST_FOR_USAS.csv")
    s3_client.upload_file(csv_file_path, defc_model.BUCKET_NAME, defc_model.RELATIVE_CSV_PATH)

    yield defc_model.RELATIVE_CSV_PATH

    s3_client.delete_object(Bucket=defc_model.BUCKET_NAME, Key=defc_model.RELATIVE_CSV_PATH)


@pytest.fixture(scope="function")
def raw_defc_mapping_file():
    # Mimic placing the raw DEFC mapping file in the expected location (directly or copied from another bucket)
    defc_mapping_model = LAKEHOUSE_MODELS["gold.defc_mapping"]()
    s3_client = _get_boto3("client", "s3")
    csv_file_path = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "DEFC_MAPPING.csv")
    s3_client.upload_file(csv_file_path, defc_mapping_model.BUCKET_NAME, defc_mapping_model.RELATIVE_CSV_PATH)

    yield defc_mapping_model.RELATIVE_CSV_PATH

    s3_client.delete_object(Bucket=defc_mapping_model.BUCKET_NAME, Key=defc_mapping_model.RELATIVE_CSV_PATH)


def test_load_defc(
    raw_defc_file: str,
    raw_defc_mapping_file: str,
    setup_teardown_buckets: List[str],
    external_data_load_dates: str,
):
    # Bronze DEFC
    defc_bronze_model = LAKEHOUSE_MODELS["bronze.defc"]()

    assert defc_bronze_model.exists()
    df = defc_bronze_model.to_pandas_df()
    assert df is not None and not df.empty
    assert df.loc[df["DEFC_CODE"] == "S", "DEFC_TITLE"].values[0] == "Disaster PL 116-260"

    # DEFC Mapping
    defc_mapping_model = LAKEHOUSE_MODELS["gold.defc_mapping"]()

    assert defc_mapping_model.exists()
    df = defc_mapping_model.to_pandas_df()
    assert df is not None and not df.empty
    assert df.loc[df["code"] == "L", "group"].values[0] == "covid_19"

    # Gold DEFC
    defc_gold_model = LAKEHOUSE_MODELS["gold.defc"]()
    defc_gold_model.initialize(recreate=True)

    defc_gold.main()

    assert defc_gold_model.exists()
    df = defc_gold_model.to_pandas_df()
    assert df is not None and not df.empty
    assert df.loc[df["code"] == "L", "public_laws"].values[0] == ["Emergency P.L. 116-123"]

    # Confirming the external load date was updated
    edld_model = LAKEHOUSE_MODELS["gold.external_data_load_date"]()

    assert edld_model.exists()
    df = edld_model.to_pandas_df()
    assert df is not None and not df.empty
    assert not df.loc[df["name"] == "gold.defc"].empty
