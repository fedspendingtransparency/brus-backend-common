import os
import pytest

from unittest.mock import patch, Mock

from brus_backend_common.helpers.uri_helper import RetrieveFileFromUri


def test_bad_scheme():
    """Tests an invalid scheme"""
    error_text = "Scheme 'ftp' isn't supported. Try one of these: ('http', 'https', 's3', 'file', '')"

    with pytest.raises(NotImplementedError) as resp_except:
        RetrieveFileFromUri("ftp://this.is.a.bad.scheme")

    assert str(resp_except.value) == error_text


def test_file_uri():
    """Tests a URI pointing to a file"""
    file_path = os.path.join("brus_backend_common", "tests", "helpers", "data", "file_content.csv")
    with RetrieveFileFromUri(file_path, binary_data=False).get_file_object() as local_file:
        first_line = local_file.readline()
        assert first_line == "test,content\n"

    app_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    file_path = "file://" + os.path.join(app_dir, file_path)
    with RetrieveFileFromUri(file_path, binary_data=False).get_file_object() as local_file:
        first_line = local_file.readline()
        assert first_line == "test,content\n"


@patch("requests.get")
def test_http_uri(requests_mock):
    """Tests a URI using a url"""
    mock_obj = Mock()
    mock_obj.content = b"test,content"
    requests_mock.return_value = mock_obj

    with RetrieveFileFromUri("http://this.is.url/file.csv").get_file_object() as url_file:
        first_line = url_file.readline()
        assert first_line == b"test,content"

    with RetrieveFileFromUri("https://this.is.url/file.csv").get_file_object() as url_file:
        first_line = url_file.readline()
        assert first_line == b"test,content"
