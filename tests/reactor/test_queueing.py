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
import weakref

import async_timeout
import pytest

from kopf.reactor.queueing import EOS, watcher


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
async def test_watchevent_demultiplexing(worker_mock, timer, resource, processor,
                                         settings, stream, events, uids, cnts):
    """ Verify that every unique uid goes into its own queue+worker, which are never shared. """

    # Override the default timeouts to make the tests faster.
    settings.batching.idle_timeout = 100  # should not be involved, fail if it is
    settings.batching.exit_timeout = 100  # should exit instantly, fail if it didn't
    settings.batching.batch_window = 100  # should not be involved, fail if it is

    # Inject the events of unique objects - to produce few streams/workers.
    stream.feed(events)
    stream.close()

    # Run the watcher (near-instantly and test-blocking).
    with timer:
        await watcher(
            namespace=None,
            resource=resource,
            settings=settings,
            processor=processor,
        )

    # Extra-check: verify that the real workers were not involved:
    # they would do batching, which is absent in the mocked workers.
    assert timer.seconds < settings.batching.batch_window

    # The processor must not be called by the watcher, only by the worker.
    # But the worker (even if mocked) must be called & awaited by the watcher.
    assert not processor.awaited
    assert not processor.called
    assert worker_mock.awaited

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


@pytest.mark.parametrize('uids, vals, events', [

    pytest.param(['uid1'], ['b'], [
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid1'}, 'spec': 'a'}},
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid1'}, 'spec': 'b'}},
    ], id='the same'),

    pytest.param(['uid1', 'uid2'], ['a', 'b'], [
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid1'}, 'spec': 'a'}},
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid2'}, 'spec': 'b'}},
    ], id='distinct'),

    pytest.param(['uid1', 'uid2', 'uid3'], ['e', 'd', 'f'], [
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid1'}, 'spec': 'a'}},
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid2'}, 'spec': 'b'}},
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid1'}, 'spec': 'c'}},
        {'type': 'MODIFIED', 'object': {'metadata': {'uid': 'uid2'}, 'spec': 'd'}},
        {'type': 'DELETED', 'object': {'metadata': {'uid': 'uid1'}, 'spec': 'e'}},
        {'type': 'DELETED', 'object': {'metadata': {'uid': 'uid3'}, 'spec': 'f'}},
    ], id='mixed'),

])
@pytest.mark.usefixtures('watcher_limited')
async def test_watchevent_batching(settings, resource, processor, timer,
                                   stream, events, uids, vals, event_loop):
    """ Verify that only the last event per uid is actually handled. """

    # Override the default timeouts to make the tests faster.
    settings.batching.idle_timeout = 100  # should not be involved, fail if it is
    settings.batching.exit_timeout = 100  # should exit instantly, fail if it didn't
    settings.batching.batch_window = 0.3  # the time period being tested (make bigger than overhead)

    # Inject the events of unique objects - to produce few streams/workers.
    stream.feed(events)
    stream.close()

    # Run the watcher (near-instantly and test-blocking).
    with timer:
        await watcher(
            namespace=None,
            resource=resource,
            settings=settings,
            processor=processor,
        )

    # Should be batched strictly once (never twice). Note: multiple uids run concurrently,
    # so they all are batched in parallel, and the timing remains the same.
    assert timer.seconds > settings.batching.batch_window * 1
    assert timer.seconds < settings.batching.batch_window * 2

    # Was the processor called at all? Awaited as needed for async fns?
    assert processor.awaited

    # Was it called only once per uid? Only with the latest event?
    # Note: the calls can be in arbitrary order, not as we expect then.
    assert processor.call_count == len(uids)
    assert processor.call_count == len(vals)
    expected_uid_val_pairs = set(zip(uids, vals))
    actual_uid_val_pairs = set((
            kwargs['raw_event']['object']['metadata']['uid'],
            kwargs['raw_event']['object']['spec'])
            for args, kwargs in processor.call_args_list)
    assert actual_uid_val_pairs == expected_uid_val_pairs


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
async def test_garbage_collection_of_streams(settings, stream, events, unique, worker_spy):

    # Override the default timeouts to make the tests faster.
    settings.batching.exit_timeout = 100  # should exit instantly, fail if it didn't
    settings.batching.idle_timeout = .05  # finish workers faster, but not as fast as batching
    settings.batching.batch_window = .01  # minimize the effects of batching (not our interest)
    settings.watching.reconnect_backoff = 1.0  # to prevent src depletion

    # Inject the events of unique objects - to produce few streams/workers.
    stream.feed(events)
    stream.close()

    # Give it a moment to populate the streams and spawn all the workers.
    # Intercept and remember _any_ seen dict of streams for further checks.
    while worker_spy.call_count < unique:
        await asyncio.sleep(0.001)  # give control to the loop
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
        settings.batching.batch_window +  # depleting the queues.
        settings.batching.idle_timeout +  # idling on empty queues.
        1.0)  # the code itself takes time: add a max tolerable delay.
    with contextlib.suppress(asyncio.TimeoutError):
        async with async_timeout.timeout(allowed_timeout):
            async with signaller:
                await signaller.wait_for(lambda: not streams)

    # The mutable(!) streams dict is now empty, i.e. garbage-collected.
    assert len(streams) == 0

    # Let the workers to actually exit and gc their local scopes with variables.
    # The jobs can take a tiny moment more, but this is noticeable in the tests.
    await asyncio.sleep(0.1)

    # Truly garbage-collected? Memory freed?
    assert all([ref() is None for ref in refs])


# TODO: also add tests for the depletion of the workers pools on cancellation (+timing)
