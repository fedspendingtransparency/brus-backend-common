import pytest
import datetime as dt
import os
import shutil
import tempfile
from filecmp import dircmp
from zipfile import ZipFile

from brus_backend_common.helpers.generic import (
    fy_period_calendar_dates,
    fy,
    batch as batcher,
    step,
    zip_dir,
)

legal_dates = {
    dt.datetime(2017, 2, 2, 16, 43, 28, 377373): 2017,
    dt.date(2017, 2, 2): 2017,
    dt.datetime(2017, 10, 2, 16, 43, 28, 377373): 2018,
    dt.date(2017, 10, 2): 2018,
    "1000-09-30": 1000,
    "1000-10-01": 1001,
    "09-30-2000": 2000,
    "10-01-2000": 2001,
    "10-01-01": 2002,
}

not_dates = (0, 2017.2, "forthwith", "string", "")


def test_fy_period_calendar_dates():
    """Test successful conversions from period to dates"""
    # Test year/period that has dates in the same year
    start, end = fy_period_calendar_dates(2017, 4)
    assert start == dt.date(2017, 1, 1)
    assert end == dt.date(2017, 1, 31)

    # Test year/period that has dates in the previous year
    start, end = fy_period_calendar_dates(2017, 2)
    assert start == dt.date(2016, 11, 1)
    assert end == dt.date(2016, 11, 30)


def test_fy_period_calendar_dates_period_failure():
    """Test invalid period formats"""
    error_text = "Period must be an integer 2-12."

    # Test period that's too high
    with pytest.raises(ValueError) as resp_except:
        fy_period_calendar_dates(2017, 13)

    assert str(resp_except.value) == error_text

    # Test period that's too low
    with pytest.raises(ValueError) as resp_except:
        fy_period_calendar_dates(2017, 1)

    assert str(resp_except.value) == error_text

    # Test null period
    with pytest.raises(ValueError) as resp_except:
        fy_period_calendar_dates(2017, None)

    assert str(resp_except.value) == error_text


def test_fy_period_calendar_dates_year_failure():
    error_text = "Year must be in YYYY format."
    # Test null year
    with pytest.raises(ValueError) as resp_except:
        fy_period_calendar_dates(None, 2)

    assert str(resp_except.value) == error_text

    # Test invalid year
    with pytest.raises(ValueError) as resp_except:
        fy_period_calendar_dates(999, 2)

    assert str(resp_except.value) == error_text


@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_integer(raw_date, expected_fy):
    assert isinstance(fy(raw_date), int)


@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_correct(raw_date, expected_fy):
    assert fy(raw_date) == expected_fy


@pytest.mark.parametrize("not_date", not_dates)
def test_fy_type_exceptions(not_date):
    assert fy(None) is None

    with pytest.raises(TypeError):
        fy(not_date)


def test_batch():
    """Testing the batch function into chunks of 100"""
    full_list = list(range(0, 1000))
    initial_batch = list(range(0, 100))
    iteration = 0
    batch_size = 100
    for batch in batcher(full_list, batch_size):
        expected_batch = [x + (batch_size * iteration) for x in initial_batch]
        assert expected_batch == batch
        iteration += 1
    assert iteration == 10


def test_step():
    """Testing the step function"""
    step_list = ["A", "B", "C", "D", "E"]

    # Simple case
    assert list(step(step_list, 2, 1, include_start=False)) == [(3, "D")]
    assert list(step(step_list, 2, 1, include_start=True)) == [(2, "C")]
    assert list(step(step_list, 2, 100, include_start=False)) == [(3, "D"), (4, "E")]
    assert list(step(step_list, 2, 100, include_start=True)) == [(2, "C"), (3, "D"), (4, "E")]

    assert list(step(step_list, 2, -1, include_start=False)) == [(1, "B")]
    assert list(step(step_list, 2, -1, include_start=True)) == [(2, "C")]
    assert list(step(step_list, 2, -100, include_start=False)) == [(1, "B"), (0, "A")]
    assert list(step(step_list, 2, -100, include_start=True)) == [(2, "C"), (1, "B"), (0, "A")]

    # Outliers
    assert list(step(step_list, 0, 0, include_start=False)) == []

    assert list(step(step_list, 0, -10, include_start=False)) == []
    assert list(step(step_list, 0, -10, include_start=True)) == [(0, "A")]
    assert list(step(step_list, 1, -10, include_start=False)) == [(0, "A")]
    assert list(step(step_list, 1, -10, include_start=True)) == [(1, "B"), (0, "A")]

    assert list(step(step_list, 4, 10, include_start=False)) == []
    assert list(step(step_list, 4, 10, include_start=True)) == [(4, "E")]
    assert list(step(step_list, 3, 10, include_start=False)) == [(4, "E")]
    assert list(step(step_list, 3, 10, include_start=True)) == [(3, "D"), (4, "E")]

    with pytest.raises(IndexError) as neg_exception:
        list(step(step_list, 10, 10, include_start=True))
    assert str(neg_exception.value) == "Start must be in range of the 0-indexed list."

    with pytest.raises(IndexError) as neg_exception:
        list(step(step_list, -10, -10, include_start=True))
    assert str(neg_exception.value) == "Start must be in range of the 0-indexed list."


def test_zip_dir():
    """Testing creating a zip with the zip_dir function"""
    # make a directory with a couple files
    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir_path = os.path.join(temp_dir, "test directory")
        os.mkdir(test_dir_path)
        test_files = {
            "test file a.txt": "TEST",
            "test file b.txt": "FILES",
            "test file c.txt": "abcd",
        }
        for test_file_path, test_file_content in test_files.items():
            with open(os.path.join(test_dir_path, test_file_path), "w") as test_file:
                test_file.write(test_file_content)

        # zip it
        test_zip_path = zip_dir(test_dir_path, "test zip")

        # keep the original directory and files to compare
        os.rename(test_dir_path, "{} original".format(test_dir_path))

        assert str(test_zip_path) == os.path.join(temp_dir, "test zip.zip")

        # confirm zip inside has the files
        ZipFile(test_zip_path).extractall(temp_dir)
        assert os.path.exists(test_dir_path)
        dir_comp = dircmp("{} original".format(test_dir_path), test_dir_path)
        assert dir_comp.left_only == []
        assert dir_comp.right_only == []
        assert dir_comp.diff_files == []

        # cleanup
        os.remove(test_zip_path)
        shutil.rmtree(test_dir_path)
        shutil.rmtree("{} original".format(test_dir_path))
