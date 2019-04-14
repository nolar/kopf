import asyncio
import time

import asynctest
import pytest
import pytest_mock

from kopf.reactor.registry import Resource


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
