import asyncio

import pytest

from kopf._core.engines.posting import event_queue_loop_var, event_queue_var


@pytest.fixture(autouse=True)
def _caplog_all_levels(caplog):
    caplog.set_level(0)


# We can get a properly scoped running loop of the test only in the async fixture.
@pytest.fixture
async def _event_queue_running_loop():
    return asyncio.get_running_loop()


@pytest.fixture(autouse=True)
def event_queue_loop(_event_queue_running_loop):  # must be sync-def
    token = event_queue_loop_var.set(_event_queue_running_loop)
    try:
        yield
    finally:
        event_queue_loop_var.reset(token)


@pytest.fixture(autouse=True)
def event_queue():
    queue = asyncio.Queue()
    token = event_queue_var.set(queue)
    try:
        yield queue
    finally:
        event_queue_var.reset(token)
