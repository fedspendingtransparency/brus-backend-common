import os

from pytest import Session


def is_pytest_xdist_parallel_sessions() -> bool:
    """Return True if the current tests executing are running in a pytest-xdist parallel test session,
    even if only 1 worker (1 master, and 1 worker) are configured"""
    worker_count = int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", 0))
    return worker_count > 0


def is_pytest_xdist_master_process(session: Session):
    """Return True if running in pytest-xdist parallel test sessions and the session given is from the 'master'
    process, and not a session of one of the spawned workers.

    This is useful if needing to orchestration cross-session (cross-worker) setup and teardown of fixtures that
    should only run once, which can be done from the master process.
    See: https://pytest-xdist.readthedocs.io/en/stable/how-to.html#making-session-scoped-fixtures-execute-only-once
    And: https://github.com/pytest-dev/pytest-xdist/issues/271#issuecomment-826396320
    """
    workerinput = getattr(session.config, "workerinput", None)
    return is_pytest_xdist_parallel_sessions() and workerinput is None


def is_safe_for_xdist_setup_or_teardown(session: Session, worker_id: str):
    """Use this in any session fixture whose setup must run EXACTLY ONCE for ALL workers
    if there are multiple parallel workers (e.g. via a pytest-xdist run with -n X or --numprocesses=X),
    and then only perform that exactly-once setup logic when this evalutes to True.
    For teardown, to ensure this cleanup runs after all workers complete their session, add cleanup logic to the pytest
    ``pytest_sessionfinish(session, exitstatus)`` fixture defined in this module, and guard it with this

    Example:
        Setup
        >>> @pytest.fixture(scope="session")
        >>> def do_some_setup_for_all_tests(session, worker_id):
        >>>     if not is_safe_for_xdist_setup_or_teardown(session, worker_id):
        >>>         yield
        >>>     else:
        >>>         # ... do setup here exactly once

        TearDown:
        >>> def pytest_sessionfinish(session, exitstatus):
        >>>     worker_id = get_xdist_worker_id(session)
        >>>     if is_safe_for_xdist_setup_or_teardown(session, worker_id):
        >>>         # ... do teardown here
    """
    return is_pytest_xdist_master_process(session) or worker_id == "master" or not worker_id
