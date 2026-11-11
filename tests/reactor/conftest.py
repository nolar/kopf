import asyncio
import functools
import inspect
from unittest.mock import AsyncMock

import pytest

from kopf._cogs.clients.watching import infinite_watch
from kopf._core.reactor.queueing import WorkerPool, watcher

STREAM_WITH_ERROR_410GONE_ONLY = (
    {'type': 'ERROR', 'object': {'code': 410}},
)


@pytest.fixture(autouse=True)
def _enforced_api_server(fake_vault, enforced_session, resource):
    pass


@pytest.fixture()
def processor():
    """ A mock for processor---to be checked if the handler has been called. """
    return AsyncMock(return_value=None)


@pytest.fixture()
def worker_pool_spy(mocker):
    """ Spy on the watcher: actually call it, but expose the mocked calls. """
    # We need not only the spy on the class itself, but on every of its methods.
    for name, _ in inspect.getmembers(WorkerPool, inspect.isfunction):
        if not name.startswith('__'):
            mocker.spy(WorkerPool, name)

    from kopf._core.reactor import queueing
    return mocker.spy(queueing, 'WorkerPool')


@pytest.fixture()
def worker_mock(mocker, worker_pool_spy):
    """ Prevent the queue consumption, so that the queues could be checked. """
    # It is already a spy, but we need it dysfunctional (no-op) entirely.
    return mocker.patch.object(WorkerPool, 'worker')


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
async def watcher_in_background(settings, resource, worker_pool_spy, kmock, namespace, processor):

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

    # Let it truly spawn and start running, create all its data structures (the worker pool mainly).
    await asyncio.sleep(0)

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
