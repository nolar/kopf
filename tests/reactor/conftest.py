import asyncio
import functools
from unittest.mock import AsyncMock

import pytest

from kopf._cogs.clients.watching import infinite_watch
from kopf._core.reactor.queueing import watcher, worker as original_worker


@pytest.fixture(autouse=True)
def _autouse_resp_mocker(resp_mocker):
    pass


@pytest.fixture()
def processor():
    """ A mock for processor -- to be checked if the handler has been called. """
    return AsyncMock(return_value=None)


@pytest.fixture()
def worker_spy(mocker):
    """ Spy on the watcher: actually call it, but provide the mock-fields. """
    spy = AsyncMock(spec=original_worker, wraps=original_worker)
    return mocker.patch('kopf._core.reactor.queueing.worker', spy)


@pytest.fixture()
def worker_mock(mocker):
    """ Prevent the queue consumption, so that the queues could be checked. """
    return mocker.patch('kopf._core.reactor.queueing.worker')


@pytest.fixture()
def watcher_limited(mocker, settings):
    """ Make event streaming finite, watcher exits after depletion. """
    settings.watching.reconnect_backoff = 0
    mocker.patch('kopf._cogs.clients.watching.infinite_watch',
                 new=functools.partial(infinite_watch, _iterations=1))


@pytest.fixture()
async def watcher_in_background(settings, resource, worker_spy, stream, namespace, processor):

    # Prevent any real streaming for the very beginning, before it even starts.
    stream.feed([], namespace=None)

    # Spawn a watcher in the background.
    coro = watcher(
        namespace=namespace,
        resource=resource,
        settings=settings,
        processor=processor,
    )
    task = asyncio.create_task(coro)

    try:
        # Go for a test.
        yield
    finally:
        # Terminate the watcher to cleanup the loop.
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass  # cancellations are expected at this point
