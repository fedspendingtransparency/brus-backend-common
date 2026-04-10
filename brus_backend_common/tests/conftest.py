import os
import pytest
import sys
import tempfile
from pytest import ExitCode
from typing import List

from xdist.plugin import get_xdist_worker_id

from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.models import LAKEHOUSE_MODELS
from brus_backend_common.tests.conftest_spark import *  # noqa
from brus_backend_common.tests.conftest_helpers import (
    is_pytest_xdist_master_process,
    is_safe_for_xdist_setup_or_teardown,
)


def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: List[pytest.Item]) -> None:
    """A global built-in fixture to pytest that is called at collection, providing a hook to modify collected items.

    In this case, used to add specific marks on tests to allow running groups/sub-groups of tests. These marks added
    here need to be declared for pytest in pyproject.toml

    Args:
        session: pytest test session object, holding details of the invoked pytest run
        config: pytest Config object, holding config details of the inovked pytest run
        items: List of tests of type ``pytest.Item`` that were collected for this pytest session
    """
    for item in items:
        # Mark all tests using the spark fixture as "spark".
        # Can be selected with -m spark or deselected with -m (not spark)
        if "spark" in getattr(item, "fixturenames", ()):
            item.add_marker("spark")


def pytest_sessionfinish(session, exitstatus):
    """A global built-in fixture to pytest that is called when all tests in a session complete.
    For parallel execution with xdist, this function is called once in each worker,
    strictly before it is called on the master process.
    NOTE: If you need to log/print, do so to sys.__stderr__ (sys.stderr is captured at this point)
    """
    worker_id = get_xdist_worker_id(session)
    if is_safe_for_xdist_setup_or_teardown(session, worker_id):
        if is_pytest_xdist_master_process(session):
            print("\nRunning pytest_sessionfinish while exiting the xdist 'master' process", file=sys.__stderr__)
        else:
            print(
                "\nRunning pytest_sessionfinish in single process execution (no xdist parallel test sessions)",
                file=sys.__stderr__,
            )
        # Add cleanup below
        pass
    else:
        print(
            f"\nRunning pytest_sessionfinish while exiting the xdist worker process with id = {worker_id}",
            file=sys.__stderr__,
        )
        # Possible per-worker or worker-specific cleanup below. Rare and not recommended to do this.
        pass

    # If no tests found, don't fail the run
    if exitstatus == ExitCode.NO_TESTS_COLLECTED:
        session.exitstatus = 0


@pytest.fixture
def temp_file_path():
    """
    If you need a temp file for something... anything... but you don't want
    the file to actually exist and you don't want to deal with managing its
    lifetime, use this fixture.
    """
    # This actually creates the temporary file, but once the context manager
    # exits, it will delete the file leaving the filename free for you to use.
    with tempfile.NamedTemporaryFile() as tf:
        path = tf.name

    yield path

    # For convenience.  Don't care if it fails.
    try:
        os.remove(path)
    except Exception:
        pass


# On a per function basis to prevent any sort of lingering loader confusion between tests
# Should just be a simple csv creation, upload, and deletion per test
@pytest.fixture(scope="function")
def external_data_load_dates():
    raw_eld_model = LAKEHOUSE_MODELS["bronze.external_data_load_date"]()
    s3_client = _get_boto3("client", "s3")

    raw_eld_model.initialize(recreate=True)

    yield raw_eld_model.RELATIVE_CSV_PATH

    s3_client.delete_object(Bucket=raw_eld_model.BUCKET_NAME, Key=raw_eld_model.RELATIVE_CSV_PATH)
