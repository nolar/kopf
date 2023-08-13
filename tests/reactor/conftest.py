import asyncio
import functools
from unittest.mock import AsyncMock

import pytest

from kopf._cogs.clients.watching import infinite_watch
from kopf._core.reactor.queueing import watcher, worker as original_worker

STREAM_WITH_ERROR_410GONE_ONLY = (
    {'type': 'ERROR', 'object': {'code': 410}},
)


@pytest.fixture(autouse=True)
def _enforced_api_server(fake_vault, enforced_session, resource):
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
def watcher_limited(kmock, mocker, settings):
    """ Make event streaming finite, watcher exits after depletion. """
    settings.watching.reconnect_backoff = 0
    mocker.patch('kopf._cogs.clients.watching.infinite_watch',
                 new=functools.partial(infinite_watch, _iterations=1))

    # Also ensure that any watches DO terminate the infinite/continuous watch-stream
    # when there are no explicitly added reactions left.
    (kmock['watch'] ** -50) << STREAM_WITH_ERROR_410GONE_ONLY


@pytest.fixture()
async def watcher_in_background(settings, resource, worker_spy, kmock, namespace, processor):

    # Prevent any real streaming for the very beginning, before it even starts.
    kmock['watch', resource] << ()

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
