import asyncio
import re
import time

import asynctest
import pytest
import pytest_mock

from kopf.reactor.registries import Resource


# Make all tests in this directory and below asyncio-compatible by default.
def pytest_collection_modifyitems(items):
    for item in items:
        if asyncio.iscoroutinefunction(item.function):
            item.add_marker('asyncio')


# Substitute the regular mock with the async-aware mock in the `mocker` fixture.
@pytest.fixture(scope='session', autouse=True)
def enforce_asyncio_mocker():
    pytest_mock._get_mock_module = lambda config: asynctest


@pytest.fixture()
def resource():
    """ The resource used in the tests. Usually mocked, so it does not matter. """
    return Resource('zalando.org', 'v1', 'kopfexamples')

#
# Helpers for the timing checks.
#

@pytest.fixture()
def timer():
    return Timer()


class Timer(object):
    """
    A helper context manager to measure the time of the code-blocks.
    Also, supports direct comparison with time-deltas and the numbers of seconds.

    Usage:

        with Timer() as timer:
            do_something()
            print(f"Executing for {timer.seconds}s already.")
            do_something_else()

        print(f"Executed in {timer.seconds}s.")
        assert timer < 5.0
    """

    def __init__(self):
        super().__init__()
        self._ts = None
        self._te = None

    @property
    def seconds(self):
        if self._ts is None:
            return None
        elif self._te is None:
            return time.perf_counter() - self._ts
        else:
            return self._te - self._ts

    def __repr__(self):
        status = 'new' if self._ts is None else 'running' if self._te is None else 'finished'
        return f'<Timer: {self.seconds}s ({status})>'

    def __enter__(self):
        self._ts = time.perf_counter()
        self._te = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._te = time.perf_counter()

    def __int__(self):
        return int(self.seconds)

    def __float__(self):
        return float(self.seconds)

#
# Helpers for the logging checks.
#

@pytest.fixture()
def assert_logs(caplog):
    """
    A function to assert the logs are present (by pattern).

    The listed message patterns MUST be present, in the order specified.
    Some other log messages can also be present, but they are ignored.
    """
    def assert_logs_fn(patterns, prohibited=[], strict=False):
        __traceback_hide__ = True
        remaining_patterns = list(patterns)
        for message in caplog.messages:
            # The expected pattern is at position 0.
            # Looking-ahead: if one of the following patterns matches, while the
            # 0th does not, then the log message is missing, and we fail the test.
            for idx, pattern in enumerate(remaining_patterns):
                m = re.search(pattern, message)
                if m:
                    if idx == 0:
                        remaining_patterns[:1] = []
                        break  # out of `remaining_patterns` cycle
                    else:
                        skipped_patterns = remaining_patterns[:idx]
                        raise AssertionError(f"Few patterns were skipped: {skipped_patterns!r}")
                elif strict:
                    raise AssertionError(f"Unexpected log message: {message!r}")

            # Check that the prohibited patterns do not appear in any message.
            for pattern in prohibited:
                m = re.search(pattern, message)
                if m:
                    raise AssertionError(f"Prohibited log pattern found: {message!r} ~ {pattern!r}")

        # If all patterns have been matched in order, we are done.
        # if some are left, but the messages are over, then we fail.
        if remaining_patterns:
            raise AssertionError(f"Few patterns were missed: {remaining_patterns!r}")

    return assert_logs_fn
