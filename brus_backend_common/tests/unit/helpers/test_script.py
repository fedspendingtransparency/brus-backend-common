import json
import pytest
import requests
import requests_mock
import xmltodict
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
