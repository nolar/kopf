import asyncio

import pytest
from asynctest import CoroutineMock

from kopf.k8s.watching import streaming_watch
from kopf.reactor.queueing import watcher
from kopf.reactor.queueing import worker as original_worker


@pytest.fixture()
def stream(mocker):
    """ A mock for the stream of events as if returned by K8s client. """
    stream = mocker.patch('kubernetes.watch.Watch.stream')
    return stream


@pytest.fixture()
def handler():
    """ A mock for handler -- to be checked if the handler has been called. """
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
def watcher_limited(mocker):
    """ Make event streaming finite, watcher exits after depletion. """
    mocker.patch('kopf.k8s.watching.infinite_watch', new=streaming_watch)


@pytest.fixture()
def watcher_in_background(resource, handler, event_loop, worker_spy, stream):

    # Prevent any real streaming for the very beginning, before it even starts.
    stream.return_value = iter([])

    # Spawn a watcher in the background.
    coro = watcher(
        namespace=None,
        resource=resource,
        handler=handler,
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
