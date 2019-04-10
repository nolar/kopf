import asyncio

import asynctest
import pytest
import pytest_mock


# Make all tests in this directory and below asyncio-compatible by default.
def pytest_collection_modifyitems(items):
    for item in items:
        if asyncio.iscoroutinefunction(item.function):
            item.add_marker('asyncio')


# Substitute the regular mock with the async-aware mock in the `mocker` fixture.
@pytest.fixture(scope='session', autouse=True)
def enforce_asyncio_mocker():
    pytest_mock._get_mock_module = lambda config: asynctest
