import os
import pytest
import numpy as np
from typing import List

from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.scripts.loaders import agency_gold
from brus_backend_common.models import LAKEHOUSE_MODELS


@pytest.fixture(scope="function")
def raw_agency_file():
    # Mimic placing the raw agency codes file in the expected location (directly or copied from another bucket)
    agency_model = LAKEHOUSE_MODELS["bronze.agency_codes"]()
    s3_client = _get_boto3("client", "s3")
    csv_file_path = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "agency_codes.csv")
    s3_client.upload_file(csv_file_path, agency_model.BUCKET_NAME, agency_model.RELATIVE_CSV_PATH)

    yield agency_model.RELATIVE_CSV_PATH

    s3_client.delete_object(Bucket=agency_model.BUCKET_NAME, Key=agency_model.RELATIVE_CSV_PATH)


def test_load_agency(
    raw_agency_file: str,
    setup_teardown_buckets: List[str],
    external_data_load_dates: str,
):
    # Base agency codes model
    agency_codes_model = LAKEHOUSE_MODELS["bronze.agency_codes"]()

    assert agency_codes_model.exists()
    raw_df = agency_codes_model.to_pandas_df()
    assert raw_df is not None and not raw_df.empty

    # Gold CGAC
    cgac_gold_model = LAKEHOUSE_MODELS["gold.cgac"]()
    cgac_gold_model.initialize(recreate=True)

    # Gold FREC
    frec_gold_model = LAKEHOUSE_MODELS["gold.frec"]()
    frec_gold_model.initialize(recreate=True)

    # Gold Subtier
    subtier_gold_model = LAKEHOUSE_MODELS["gold.subtier_agency"]()
    subtier_gold_model.initialize(recreate=True)

    agency_gold.main()

    # CGAC tests
    assert cgac_gold_model.exists()

    cgac_df = cgac_gold_model.to_pandas_df()
    assert cgac_df is not None and not cgac_df.empty

    # remove np.nan values for testing
    cgac_df = cgac_df.replace({np.nan: None})
    # Test basic cgac
    assert cgac_df.loc[cgac_df["cgac_code"] == "000", "agency_name"].values[0] == "U.S. Congress"
    assert cgac_df.loc[cgac_df["cgac_code"] == "000", "agency_abbreviation"].values[0] == "CONGRESS"
    assert cgac_df.loc[cgac_df["cgac_code"] == "000", "display_name"].values[0] == "U.S. Congress (CONGRESS)"

    # Test cgac without an abbreviation
    assert cgac_df.loc[cgac_df["cgac_code"] == "017", "agency_name"].values[0] == "Department of the Navy"
    assert cgac_df.loc[cgac_df["cgac_code"] == "017", "agency_abbreviation"].values[0] is None
    assert cgac_df.loc[cgac_df["cgac_code"] == "017", "display_name"].values[0] == "Department of the Navy (nan)"

    # FREC tests
    assert frec_gold_model.exists()

    frec_df = frec_gold_model.to_pandas_df()
    assert frec_df is not None and not frec_df.empty

    # remove np.nan values for testing
    frec_df = frec_df.replace({np.nan: None})
    # Test basic frec
    assert frec_df.loc[frec_df["frec_code"] == "0100", "cgac_code"].values[0] == "001"
    assert frec_df.loc[frec_df["frec_code"] == "0100", "agency_name"].values[0] == "Architect of the Capitol"
    assert frec_df.loc[frec_df["frec_code"] == "0100", "agency_abbreviation"].values[0] == "AOC"
    assert frec_df.loc[frec_df["frec_code"] == "0100", "display_name"].values[0] == "Architect of the Capitol (AOC)"

    # Test frec without an abbreviation, also making sure we're consistent with choosing the later CGAC
    assert frec_df.loc[frec_df["frec_code"] == "0000", "cgac_code"].values[0] == "002"
    assert frec_df.loc[frec_df["frec_code"] == "0000", "agency_name"].values[0] == "Non - Reporting"
    assert frec_df.loc[frec_df["frec_code"] == "0000", "agency_abbreviation"].values[0] is None
    assert frec_df.loc[frec_df["frec_code"] == "0000", "display_name"].values[0] == "Non - Reporting (nan)"

    # Subtier tests
    assert subtier_gold_model.exists()

    subtier_df = subtier_gold_model.to_pandas_df()
    assert subtier_df is not None and not subtier_df.empty

    # remove np.nan values for testing
    subtier_df = subtier_df.replace({np.nan: None})
    # Test priority 1 subtier
    assert subtier_df.loc[subtier_df["subtier_code"] == "0010", "cgac_code"].values[0] == "000"
    assert subtier_df.loc[subtier_df["subtier_code"] == "0010", "frec_code"].values[0] == "0000"
    assert subtier_df.loc[subtier_df["subtier_code"] == "0010", "subtier_name"].values[0] == "The Senate"
    assert subtier_df.loc[subtier_df["subtier_code"] == "0010", "priority"].values[0] == 1
    assert not subtier_df.loc[subtier_df["subtier_code"] == "0010", "is_frec"].values[0]

    # Test priority 2 subtier
    assert subtier_df.loc[subtier_df["subtier_code"] == "0011", "cgac_code"].values[0] == "000"
    assert subtier_df.loc[subtier_df["subtier_code"] == "0011", "frec_code"].values[0] == "0000"
    assert (
        subtier_df.loc[subtier_df["subtier_code"] == "0011", "subtier_name"].values[0]
        == "The United States Senate Sergeant at Arms"
    )
    assert subtier_df.loc[subtier_df["subtier_code"] == "0011", "priority"].values[0] == 2
    assert not subtier_df.loc[subtier_df["subtier_code"] == "0011", "is_frec"].values[0]

    # Test frec subtier
    assert subtier_df.loc[subtier_df["subtier_code"] == "1103", "cgac_code"].values[0] == "011"
    assert subtier_df.loc[subtier_df["subtier_code"] == "1103", "frec_code"].values[0] == "1100"
    assert (
        subtier_df.loc[subtier_df["subtier_code"] == "1103", "subtier_name"].values[0]
        == "Office of Management and Budget"
    )
    assert subtier_df.loc[subtier_df["subtier_code"] == "1103", "priority"].values[0] == 2
    assert subtier_df.loc[subtier_df["subtier_code"] == "1103", "is_frec"].values[0]

    # Confirming the external load date was updated
    edld_model = LAKEHOUSE_MODELS["gold.external_data_load_date"]()

    assert edld_model.exists()
    edld_df = edld_model.to_pandas_df()
    assert edld_df is not None and not edld_df.empty
    assert not edld_df.loc[edld_df["name"] == "gold.cgac"].empty
    assert not edld_df.loc[edld_df["name"] == "gold.frec"].empty
    assert not edld_df.loc[edld_df["name"] == "gold.subtier_agency"].empty
