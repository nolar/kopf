import asyncio
import functools

import pytest
from asynctest import CoroutineMock

from kopf.clients.watching import infinite_watch
from kopf.reactor.queueing import watcher, worker as original_worker


@pytest.fixture(autouse=True)
def _autouse_resp_mocker(resp_mocker):
    pass


@pytest.fixture()
def processor():
    """ A mock for processor -- to be checked if the handler has been called. """
    return CoroutineMock()


@pytest.fixture()
def worker_spy(mocker):
    """ Spy on the watcher: actually call it, but provide the mock-fields. """
    spy = CoroutineMock(spec=original_worker, wraps=original_worker)
    return mocker.patch('kopf.reactor.queueing.worker', spy)


@pytest.fixture()
def worker_mock(mocker):
    """ Prevent the queue consumption, so that the queues could be checked. """
    return mocker.patch('kopf.reactor.queueing.worker')


@pytest.fixture()
def watcher_limited(mocker, settings):
    """ Make event streaming finite, watcher exits after depletion. """
    settings.watching.reconnect_backoff = 0
    mocker.patch('kopf.clients.watching.infinite_watch',
                 new=functools.partial(infinite_watch, _iterations=1))


@pytest.fixture()
def watcher_in_background(settings, resource, event_loop, worker_spy, stream):

    # Prevent remembering the streaming objects in the mocks.
    async def do_nothing(*args, **kwargs):
        pass

    # Prevent any real streaming for the very beginning, before it even starts.
    stream.feed([])

    # Spawn a watcher in the background.
    coro = watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=do_nothing,
    )
    task = event_loop.create_task(coro)

    try:
        # Go for a test.
        yield task
    finally:
        # Terminate the watcher to cleanup the loop.
        task.cancel()
        try:
            event_loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
