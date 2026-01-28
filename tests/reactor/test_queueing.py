"""
Only the tests from the watching (simulated) to the handling (substituted).

Excluded: the watching-streaming routines
(see ``tests_streaming.py`` and ``test_watching_*.py``).

Excluded: the causation and handling routines
(to be done later).

Used for internal control that the event queueing works are intended.
If the intentions change, the tests should be rewritten.
They are NOT part of the public interface of the framework.

NOTE: These tests also check that the bookmarks are ignored
by checking that they are not multiplexed into workers.
"""
import asyncio
import contextlib
import gc
import weakref

import pytest

from kopf._core.reactor.queueing import EOS, ObjectUid, Stream, watcher, worker


@pytest.mark.parametrize('uids, cnts, events', [

    pytest.param(['uid1'], [1], [
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid1'}}},
    ], id='single'),

    pytest.param(['uid1'], [3], [
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid1'}}},
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid1'}}},
        {'type': 'DELETED', 'object': {'metadata': {'uid': 'uid1'}}},
    ], id='multiple'),

    pytest.param(['uid1', 'uid2'], [3, 2], [
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid1'}}},
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid2'}}},
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid1'}}},
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid2'}}},
        {'type': 'DELETED', 'object': {'metadata': {'uid': 'uid1'}}},
    ], id='mixed'),

])
@pytest.mark.usefixtures('watcher_limited')
async def test_watchevent_demultiplexing(worker_mock, looptime, resource, processor,
                                         settings, stream, events, uids, cnts):
    """ Verify that every unique uid goes into its own queue+worker, which are never shared. """

    # Override the default timeouts to make the tests faster.
    settings.queueing.idle_timeout = 100  # should not be involved, fail if it is
    settings.queueing.exit_timeout = 100  # should exit instantly, fail if it didn't

    # Inject the events of unique objects - to produce a few streams/workers.
    stream.feed(events, namespace=None)
    stream.close(namespace=None)

    # Run the watcher (near-instantly and test-blocking).
    await watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=processor,
    )

    # Extra-check: verify that the real workers were not involved:
    # they would do batching, which is absent in the mocked workers.
    assert looptime == 0

    # The processor must not be called by the watcher, only by the worker.
    # But the worker (even if mocked) must be called & awaited by the watcher.
    assert processor.call_count == 0
    assert processor.await_count == 0
    assert worker_mock.await_count > 0

    # Are the worker-streams created by the watcher? Populated as expected?
    # One stream per unique uid? All events are sequential? EOS marker appended?
    assert worker_mock.call_count == len(uids)
    assert worker_mock.call_count == len(cnts)
    for uid, cnt, (args, kwargs) in zip(uids, cnts, worker_mock.call_args_list):
        key = kwargs['key']
        streams = kwargs['streams']
        assert kwargs['processor'] is processor
        assert key == (resource, uid)
        assert key in streams

        queue_events = []
        while not streams[key].backlog.empty():
            queue_events.append(streams[key].backlog.get_nowait())

        assert len(queue_events) == cnt + 1
        assert queue_events[-1] is EOS.token
        assert all(queue_event['object']['metadata']['uid'] == uid
                   for queue_event in queue_events[:-1])


@pytest.mark.parametrize('unique, events', [

    pytest.param(1, [
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid1'}}},
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid1'}}},
        {'type': 'DELETED', 'object': {'metadata': {'uid': 'uid1'}}},
    ], id='the same'),

    pytest.param(2, [
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid1'}}},
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid2'}}},
    ], id='distinct'),

])
@pytest.mark.usefixtures('watcher_in_background')
async def test_garbage_collection_of_streams(
        settings, stream, events, unique, worker_spy, namespace, processor
):

    # Override the default timeouts to make the tests faster.
    settings.queueing.exit_timeout = 999  # should exit instantly, fail if it didn't
    settings.queueing.idle_timeout = 5  # finish workers faster, but not as fast as batching
    settings.watching.reconnect_backoff = 100  # to prevent src depletion

    # Inject the events of unique objects - to produce a few streams/workers.
    stream.feed(events, namespace=namespace)
    stream.close(namespace=namespace)

    # Give it a moment to populate the streams and spawn all the workers.
    # Intercept and remember _any_ seen dict of streams for further checks.
    while worker_spy.call_count < unique:
        await asyncio.sleep(0)  # give control to the loop
    streams = worker_spy.call_args_list[-1][1]['streams']
    signaller: asyncio.Condition = worker_spy.call_args_list[0][1]['signaller']

    # The mutable(!) streams dict is now populated with the objects' streams.
    assert len(streams) != 0  # usually 1, but can be 2+ if it is fast enough.

    # Weakly remember the stream's content to make sure it is gc'ed later.
    # Note: namedtuples are not referable due to __slots__/__weakref__ issues.
    refs = [weakref.ref(val) for wstream in streams.values() for val in wstream]
    assert all([ref() is not None for ref in refs])

    # Give the workers some time to finish waiting for the events.
    # After the idle timeout is reached, they will exit and gc their streams.
    allowed_timeout = (
        settings.queueing.idle_timeout +  # idling on empty queues.
        1.0)  # the code itself takes time: add a max tolerable delay.
    with contextlib.suppress(asyncio.TimeoutError):
        async with signaller:
            await asyncio.wait_for(signaller.wait_for(lambda: not streams), timeout=allowed_timeout)

    # The mutable(!) streams dict is now empty, i.e. garbage-collected.
    assert len(streams) == 0

    # Let the workers to actually exit and gc their local scopes with variables.
    # The jobs can take a tiny moment more, but this is noticeable in the tests.
    await asyncio.sleep(0)

    # Release all remembered "pressure" events & other arguments passed to a processor.
    processor.reset_mock()

    # For PyPy: force the gc! (GC can be delayed in PyPy, unlike in CPython.)
    # https://doc.pypy.org/en/latest/cpython_differences.html#differences-related-to-garbage-collection-strategies
    gc.collect()

    # Truly garbage-collected? Memory freed?
    assert all([ref() is None for ref in refs])


async def test_stream_pressure_maintained_until_the_queue_is_empty(settings, resource, processor):

    flags: list[bool] = []
    processor.side_effect = lambda stream_pressure, **_: flags.append(stream_pressure.is_set())

    # It is very important for this test that the stream (queue+flag) is pre-constructed,
    # i.e., that it is not populated by the watcher at a random speed, but pre-populated manually.
    # Therefore, we test the worker(), not the watcher().
    key = (resource, ObjectUid('uid1'))
    stream = Stream(backlog=asyncio.Queue(), pressure=asyncio.Event())
    stream.backlog.put_nowait({'type': 'ADDED', 'object': {'metadata': {'uid': 'uid1'}}})
    stream.backlog.put_nowait({'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid1'}}})
    stream.backlog.put_nowait({'type': 'DELETED', 'object': {'metadata': {'uid': 'uid1'}}})
    stream.pressure.set()
    await worker(
        signaller=asyncio.Condition(),  # irrelevant
        settings=settings,
        processor=processor,
        streams={key: stream},
        key=key,
    )

    assert flags == [True, True, False]
    assert not stream.pressure.is_set()


# TODO: also add tests for the depletion of the workers pools on cancellation (+timing)
