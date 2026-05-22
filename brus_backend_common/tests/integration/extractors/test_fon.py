import json
import os
import requests
from typing import List

import pandas as pd
import requests_mock

from requests_mock.response import _Context
from unittest.mock import patch

from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.models import LAKEHOUSE_MODELS
from brus_backend_common.scripts.extractors import fon

MOCK_BATCH_SIZE = 2


def extract_mock_fon_json(resp_num: int):
    resp_path = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", f"fon_resp_{resp_num}.json")
    with open(resp_path, "r") as resp_file:
        resp_data = json.load(resp_file)
    return resp_data


def mock_fon_response(request: requests.Request, context: _Context):
    incoming_data = request.json()

    resp_num = (
        int(incoming_data.get("startRecordNum") / MOCK_BATCH_SIZE) + 1
    )  # 0 -> 1 (first resp), 2 -> 2 (second resp)
    return extract_mock_fon_json(resp_num)


@patch("brus_backend_common.scripts.extractors.fon.BATCH_SIZE", MOCK_BATCH_SIZE)
def test_extract_fon_data():
    with requests_mock.Mocker() as m:
        m.post(fon.FON_URL, json=mock_fon_response)
        fon_data = fon.extract_fon_data()

    assert isinstance(fon_data, pd.DataFrame)

    # Confirm it pulled both pages
    assert len(fon_data) == 4

    # Checking the raw json with the combined json to be used for FON Bronze
    fon_list = []
    for resp_num in range(1, 3):
        fon_resp_json = extract_mock_fon_json(resp_num=resp_num)
        fon_list.extend(fon_resp_json["oppHits"])

    assert pd.DataFrame(fon_list).equals(fon_data)


@patch("brus_backend_common.scripts.extractors.fon.BATCH_SIZE", MOCK_BATCH_SIZE)
def test_fon(setup_teardown_buckets: List[str], external_data_load_dates: str):
    with requests_mock.Mocker() as m:
        m.post(fon.FON_URL, json=mock_fon_response)
        fon.main()

    # Check to see Bronze table is populated
    fon_bronze = LAKEHOUSE_MODELS["bronze.funding_opportunity"]()
    fon_bronze.initialize()

    assert fon_bronze.exists()
    assert fon_bronze.count() == 4
    fon_bronze_df = fon_bronze.to_pandas_df()
    assert fon_bronze_df.loc[fon_bronze_df["id"] == 51463, "agency"].values[0] == "Test Agency 4"

    # Check to see external load date's updated
    edld_model = LAKEHOUSE_MODELS["gold.external_data_load_date"]()

    assert edld_model.exists()
    df = edld_model.to_pandas_df()
    assert df is not None and not df.empty
    assert not df.loc[df["name"] == "bronze.funding_opportunity"].empty
