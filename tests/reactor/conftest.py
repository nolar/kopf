import asyncio
import dataclasses
from typing import List

import pytest
from asynctest import CoroutineMock

from kopf.clients.watching import streaming_watch
from kopf.reactor.queueing import watcher
from kopf.reactor.queueing import worker as original_worker
from kopf.structs.configuration import OperatorSettings


@pytest.fixture(autouse=True)
def _autouse_resp_mocker(resp_mocker):
    pass


@pytest.fixture()
def processor():
    """ A mock for processor -- to be checked if the handler has been called. """
    return CoroutineMock()


# Code overhead is not used, but is needed to order the fixtures: first,
# the measurement, which requires the real worker; then, the worker mocking.
@pytest.fixture()
def worker_spy(mocker, code_overhead):
    """ Spy on the watcher: actually call it, but provide the mock-fields. """
    spy = CoroutineMock(spec=original_worker, wraps=original_worker)
    return mocker.patch('kopf.reactor.queueing.worker', spy)


# Code overhead is not used, but is needed to order the fixtures: first,
# the measurement, which requires the real worker; then, the worker mocking.
@pytest.fixture()
def worker_mock(mocker, code_overhead):
    """ Prevent the queue consumption, so that the queues could be checked. """
    return mocker.patch('kopf.reactor.queueing.worker')


@pytest.fixture()
def watcher_limited(mocker):
    """ Make event streaming finite, watcher exits after depletion. """
    mocker.patch('kopf.clients.watching.infinite_watch', new=streaming_watch)


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


@dataclasses.dataclass(frozen=True)
class CodeOverhead:
    min: float
    avg: float
    max: float


@pytest.fixture(scope='session')
def _code_overhead_cache():
    return []


@pytest.fixture()
async def code_overhead(
        resource, stream, aresponses, watcher_limited, timer,
        _code_overhead_cache,
) -> CodeOverhead:
    """
    Estimate the overhead of synchronous code in the watching routines.

    The code overhead is caused by Kopf's and tests' own low-level activities:
    the code of ``watcher()``/``worker()`` itself, including a job scheduler,
    the local ``aresponses`` server, the API communication with that server
    in ``aiohttp``, serialization/deserialization in ``kopf.clients``, etc.

    The actual aspect being tested are the ``watcher()``/``worker()`` routines:
    their input/output and their timing regarding the blocking queue operations
    or explicit sleeps, not the timing of underlying low-level activities.
    So, the expected values for the durations of the call are adjusted for
    the estimated code overhead before asserting them.

    .. note::

        The tests are designed with small timeouts to run fast, so that
        the whole test-suite with thousands of tests is not delayed much.
        Once there is a way to simulate asyncio time like with ``freezegun``,
        or ``freezegun`` supports asyncio time, the problem can be solved by
        using the lengthy timeouts and ignoring the code overhead._

    The estimation of the overhead is measured by running a single-event cycle,
    which means one worker only, but with batching of events disabled. This
    ensures that only the fastest way is executed: no explicit or implicit
    sleeps are used (e.g. as in getting from an empty queue with timeouts).

    Extra 10-30% are added to the measured overhead to ensure that the future
    code executions would fit into the estimation despite the variations.

    Empirically, the overhead usually remains within the range of 50-150 ms.
    It does not depend on the number of events or unique uids in the stream.
    It does depend on the hardware used, or containers in the CI systems.

    Several dummy runs are used to average the values, to avoid fluctuation.
    The estimation happens only once per session, and is reused for all tests.
    """
    if not _code_overhead_cache:

        # Collect a few data samples to make the estimation realistic.
        overheads: List[float] = []
        for _ in range(10):

            # We feed the stream and consume the stream before we go into the tests,
            # which can feed the stream with their own events.
            stream.feed([
                {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid'}}},
            ])
            stream.close()

            # We use our own fixtures -- to not collide with the tests' fixtures.
            processor = CoroutineMock()
            settings = OperatorSettings()
            settings.batching.batch_window = 0
            settings.batching.idle_timeout = 1
            settings.batching.exit_timeout = 1

            with timer:
                await watcher(
                    namespace=None,
                    resource=resource,
                    settings=settings,
                    processor=processor,
                )

            # Ensure that everything worked as expected, i.e. the worker is not mocked,
            # and the whole code is actually executed down to the processor callback.
            assert processor.awaited, "The processor is not called for code overhead measurement."
            overheads.append(timer.seconds)

        # Reserve extra 10-30% from both sides for occasional variations.
        _code_overhead_cache.append(CodeOverhead(
            min=min(overheads) * 0.9,
            avg=sum(overheads) / len(overheads),
            max=max(overheads) * 1.1,
        ))

        # Cleanup our own endpoints, if something is left.
        aresponses._responses[:] = []

    # Uncomment for debugging of the actual timing: visible only with -s pytest option.
    # print(f"The estimated code overhead is {overhead}.")

    return _code_overhead_cache[0]
