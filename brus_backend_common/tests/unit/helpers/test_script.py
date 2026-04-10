import json
import pytest
import requests
import requests_mock
import xmltodict
import pandas as pd
import numpy as np
from requests.exceptions import HTTPError, ConnectionError

from brus_backend_common.helpers import scripts


def test_flatten_json():
    """Test flattening jsons"""
    test_json = {"a": [1, 2, 3], "b": [2, 3, 4]}
    result = {"a_0": 1, "a_1": 2, "a_2": 3, "b_0": 2, "b_1": 3, "b_2": 4}
    assert scripts.flatten_json(test_json) == result

    test_json = [{"a": [1, 2, 3]}, {"b": [2, 3, 4]}]
    result = {"0_a_0": 1, "0_a_1": 2, "0_a_2": 3, "1_b_0": 2, "1_b_1": 3, "1_b_2": 4}
    assert scripts.flatten_json(test_json) == result


def test_trim_nested_obj():
    """Test trimming nested objects"""
    test_json = [{" a ": ["1", "         2", "3     "], "b": ["  2   ", "  3", "4"]}]
    # note: it doesn't trim the keys, only the values
    result = [{" a ": ["1", "2", "3"], "b": ["2", "3", "4"]}]
    assert scripts.trim_nested_obj(test_json) == result


def test_get_with_exception_hand():
    url = "http://test.com"

    # happy raw response
    with requests_mock.Mocker() as m:
        m.get(url, text="data")
        resp = scripts.get_with_exception_hand(url, decode=False)
        assert isinstance(resp, requests.Response)
        assert resp.status_code == 200
        assert resp.text == "data"

    # happy xml
    with requests_mock.Mocker() as m:
        m.get(url, text="<entries><entry>entry</entry></entries>")
        resp_dict = scripts.get_with_exception_hand(url, resp_type="xml")
        assert isinstance(resp_dict, dict)
        assert resp_dict == {"entries": {"entry": "entry"}}

    # happy json
    with requests_mock.Mocker() as m:
        test_dict = {"a": 1, "b": 2}
        m.get(url, text=json.dumps(test_dict))
        resp_dict = scripts.get_with_exception_hand(url)
        assert isinstance(resp_dict, dict)
        assert resp_dict == test_dict

    # retry for error status codes
    with requests_mock.Mocker() as m:
        m.get(url, status_code=400)
        with pytest.raises(HTTPError):
            scripts.get_with_exception_hand(url, max_retries=1)
        assert m.call_count == 2

    with requests_mock.Mocker() as m:
        m.get(url, status_code=500)
        with pytest.raises(HTTPError):
            scripts.get_with_exception_hand(url, max_retries=1)
        assert m.call_count == 2

    # fail for connection error
    with requests_mock.Mocker() as m:
        m.get(url, exc=ConnectionError)
        with pytest.raises(ConnectionError):
            scripts.get_with_exception_hand(url, max_retries=1)
        assert m.call_count == 2

    # fail for json decoding error
    with requests_mock.Mocker() as m:
        m.get(url, text="{'a': !}")
        with pytest.raises(json.decoder.JSONDecodeError):
            scripts.get_with_exception_hand(url, max_retries=1)
        assert m.call_count == 2

    # fail for xml decoding error
    with requests_mock.Mocker() as m:
        m.get(url, text="<entries><</entries>")
        with pytest.raises(xmltodict.expat.ExpatError):
            scripts.get_with_exception_hand(url, resp_type="xml", max_retries=1)
        assert m.call_count == 2

    # fail for error in message
    with requests_mock.Mocker() as m:
        error_name = "Invalid call"
        error_message = "Missing request param X."
        m.get(url, text=f'{{"error": "{error_name}", "message": "{error_message}"}}')
        with pytest.raises(ValueError) as e:
            scripts.get_with_exception_hand(url, max_retries=1)
        assert str(e.value) == f"Error processing response: {error_name} {error_message}"
        assert m.call_count == 2

    # fail for validate_response
    with requests_mock.Mocker() as m:
        m.get(url, text="{'entries': []}")
        with pytest.raises(ValueError) as e:
            scripts.get_with_exception_hand(
                url, validate_response=lambda resp: (len(resp["entries"]) > 0), max_retries=1
            )
            assert str(e) == error_message
        assert m.call_count == 2


def test_clean_data():
    normal_df = pd.DataFrame(
        {
            "First Name": ["Tom", "Nick", "Krish", "Jack"],
            "Age": ["20", "21", "19", "18"],
            "Amount": ["1,000", "2,000", "3,000", "4,000"],
            "City": ["New York", "London", "Paris", "Tokyo"],
        }
    )
    expected_clean_df = pd.DataFrame(
        {
            "name": ["Tom", "Nick", "Krish", "Jack"],
            "age": ["20", "21", "19", "18"],
            "amount": ["1,000", "2,000", "3,000", "4,000"],
        }
    )
    field_map = {"First Name": "name", "Age": "age", "Amount": "amount"}
    field_options = {}

    # Happy path
    # Drop fields not in field map
    # Rename cols per field map
    result_df = scripts.clean_data(
        normal_df, field_map=field_map, field_options=field_options, clean_col_names=False, add_dates=False
    )
    assert result_df.equals(expected_clean_df)

    # Drop N/As, nulls, blanks, etc.
    empty_row = pd.DataFrame([[np.nan] * len(normal_df.columns)], columns=normal_df.columns)
    test_df = pd.concat([normal_df, empty_row], ignore_index=True)

    result_df = scripts.clean_data(
        test_df, field_map=field_map, field_options=field_options, clean_col_names=False, add_dates=False
    )
    assert result_df.equals(expected_clean_df)

    # Clean cols
    test_df = normal_df.copy(deep=True)
    field_map = {"first_name": "name", "age": "age", "amount": "amount"}

    result_df = scripts.clean_data(
        test_df, field_map=field_map, field_options=field_options, clean_col_names=True, add_dates=False
    )
    assert result_df.equals(expected_clean_df)

    # Field not included but in field map
    test_df = normal_df.drop(columns=["Amount"])

    with pytest.raises(ValueError) as error:
        scripts.clean_data(
            test_df, field_map=field_map, field_options=field_options, clean_col_names=True, add_dates=False
        )
    assert str(error.value) == "The following fields are required per field_map: {'amount'}"

    # Trim values and columns
    test_df = normal_df.copy(deep=True)
    test_df["First Name"] = " " + test_df["First Name"] + " "
    test_df["Age"] = " " + test_df["Age"]
    test_df["Amount"] = test_df["Amount"] + " "
    test_df["City"] = test_df["City"]

    result_df = scripts.clean_data(
        test_df, field_map=field_map, field_options=field_options, clean_col_names=True, add_dates=False
    )

    assert result_df.equals(expected_clean_df)

    # Required values check - empty file
    required = ["name", "amount"]
    empty_df = normal_df[:0]

    with pytest.raises(scripts.FailureThresholdExceededError) as error:
        scripts.clean_data(
            empty_df,
            field_map=field_map,
            field_options=field_options,
            clean_col_names=True,
            add_dates=False,
            required_values=required,
        )
    assert error.value.count == 0

    # Required values check - empty strings, nulls
    # return dropped count
    # The threshold is 1%, so we need to have an error row to make it just less than that
    df_len = 101
    test_df = pd.DataFrame(
        {
            "First Name": ["Tom"] * df_len,
            "Age": ["20"] * df_len,
            "Amount": ["1,000"] * df_len,
            "City": ["New York"] * df_len,
        }
    )
    empty_row = pd.DataFrame({"First Name": [" "], "Age": ["20"], "Amount": [np.nan], "City": ["New York"]})
    test_df = pd.concat([test_df, empty_row], ignore_index=True)

    dropped_count, result_df = scripts.clean_data(
        test_df,
        field_map=field_map,
        field_options=field_options,
        clean_col_names=True,
        add_dates=False,
        required_values=required,
        return_dropped_count=True,
    )
    assert result_df.equals(
        pd.DataFrame({"name": ["Tom"] * df_len, "age": ["20"] * df_len, "amount": ["1,000"] * df_len})
    )
    assert dropped_count == 1

    # threshold percentage fail
    # return dropped count
    test_df = empty_row

    with pytest.raises(scripts.FailureThresholdExceededError) as error:
        scripts.clean_data(
            test_df,
            field_map=field_map,
            field_options=field_options,
            clean_col_names=True,
            add_dates=False,
            required_values=required,
        )
    assert error.value.count == 1

    # pad to length, keep null, strip_commas
    test_df = pd.DataFrame(
        {
            "First Name": ["Bond, James", "Trevelyan, Alec"],
            "Age": ["34", "36"],
            "Amount": ["7", np.nan],
            "City": ["London", "London"],
        }
    )
    field_options = {
        "name": {"strip_commas": True},
        "amount": {"keep_null": True, "pad_to_length": 3},
    }
    required = ["name", "age"]
    result_df = scripts.clean_data(
        test_df,
        field_map=field_map,
        field_options=field_options,
        clean_col_names=True,
        add_dates=False,
        required_values=required,
    )
    assert result_df.equals(
        pd.DataFrame({"name": ["Bond James", "Trevelyan Alec"], "age": ["34", "36"], "amount": ["007", np.nan]})
    )

    # add created_at, updated_at
    result_df = scripts.clean_data(
        normal_df,
        field_map=field_map,
        field_options=field_options,
        clean_col_names=True,
        add_dates=True,
        required_values=required,
    )
    assert {"created_at", "updated_at"} < set(result_df.columns)
