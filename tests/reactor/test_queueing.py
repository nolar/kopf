"""
Only the tests from the watching (simulated) to the handling (substituted).

Excluded: the watching-streaming routines
(see ``tests_streaming.py`` and ``test_watching.py``).

Excluded: the causation and handling routines
(to be done later).

Used for internal control that the event queueing works are intended.
If the intentions change, the tests should be rewritten.
They are NOT part of the public interface of the framework.
"""
import asyncio
import weakref

import pytest

from kopf.reactor.queueing import watcher, EOS

# Some overhead for the synchronous logic in async tests: it also takes time.
CODE_OVERHEAD = 0.1  # 0.01 is too fast, 0.1 is too slow, 0.05 is good enough.


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
async def test_event_multiplexing(worker_mock, timer, resource, handler, stream, events, uids, cnts):
    """ Verify that every unique uid goes into its own queue+worker, which are never shared. """

    # Inject the events of unique objects - to produce few queues/workers.
    stream.return_value = iter(events)

    # Run the watcher (near-instantly and test-blocking).
    with timer:
        await watcher(
            namespace=None,
            resource=resource,
            handler=handler,
        )

    # The queues are not cleared by the mocked worker, but the worker exits fast.
    assert timer.seconds < CODE_OVERHEAD

    # The handler must not be called by the watcher, only by the worker.
    # But the worker (even if mocked) must be called & awaited by the watcher.
    assert not handler.awaited
    assert not handler.called
    assert worker_mock.awaited

    # Are the worker-queues created by the watcher? Populated as expected?
    # One queue per unique uid? All events are sequential? EOS marker appended?
    assert worker_mock.call_count == len(uids)
    assert worker_mock.call_count == len(cnts)
    for uid, cnt, (args, kwargs) in zip(uids, cnts, worker_mock.call_args_list):
        key = kwargs['key']
        queues = kwargs['queues']
        assert kwargs['handler'] is handler
        assert key == (resource, uid)
        assert key in queues

        queue_events = []
        while not queues[key].empty():
            queue_events.append(queues[key].get_nowait())

        assert len(queue_events) == cnt + 1
        assert queue_events[-1] is EOS
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
async def test_event_batching(mocker, resource, handler, timer, stream, events, uids, vals):
    """ Verify that only the last event per uid is actually handled. """

    # Override the default timeouts to make the tests faster.
    mocker.patch('kopf.config.WorkersConfig.worker_idle_timeout', 0.5)
    mocker.patch('kopf.config.WorkersConfig.worker_batch_window', 0.1)
    mocker.patch('kopf.config.WorkersConfig.worker_exit_timeout', 0.5)

    # Inject the events of unique objects - to produce few queues/workers.
    stream.return_value = iter(events)

    # Run the watcher (near-instantly and test-blocking).
    with timer:
        await watcher(
            namespace=None,
            resource=resource,
            handler=handler,
        )

    # Significantly less than the queue getting timeout, but sufficient to run.
    # 2 <= 1 pull for the event chain + 1 pull for EOS. TODO: 1x must be enough.
    from kopf import config
    assert timer.seconds < config.WorkersConfig.worker_batch_window + CODE_OVERHEAD

    # Was the handler called at all? Awaited as needed for async fns?
    assert handler.awaited

    # Was it called only once per uid? Only with the latest event?
    assert handler.call_count == len(uids)
    assert handler.call_count == len(vals)
    for uid, val, (args, kwargs) in zip(uids, vals, handler.call_args_list):
        event = kwargs['event']
        assert event['object']['metadata']['uid'] == uid
        assert event['object']['spec'] == val


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
async def test_garbage_collection_of_queues(mocker, stream, events, unique, worker_spy):

    # Override the default timeouts to make the tests faster.
    mocker.patch('kopf.config.WorkersConfig.worker_idle_timeout', 0.5)
    mocker.patch('kopf.config.WorkersConfig.worker_batch_window', 0.1)
    mocker.patch('kopf.config.WorkersConfig.worker_exit_timeout', 0.5)

    # Inject the events of unique objects - to produce few queues/workers.
    stream.return_value = iter(events)

    # Give it a moment to populate the queues and spawn all the workers.
    # Intercept and remember _any_ seen dict of queues for further checks.
    while worker_spy.call_count < unique:
        await asyncio.sleep(0.001)  # give control to the loop
    queues = worker_spy.call_args_list[-1][1]['queues']

    # The mutable(!) queues dict is now populated with the objects' queues.
    assert len(queues) != 0  # usually 1, but can be 2+ if it is fast enough.

    # Weakly remember the queues to make sure they are gc'ed later.
    refs = [weakref.ref(queue) for queue in queues.values()]
    assert all([ref() is not None for ref in refs])

    # Give the workers some time to finish waiting for the events.
    # Once the idle timeout, they will exit and gc their individual queues.
    from kopf import config
    await asyncio.sleep(config.WorkersConfig.worker_batch_window)  # depleting the queues.
    await asyncio.sleep(config.WorkersConfig.worker_idle_timeout)  # idling on empty queues.
    await asyncio.sleep(CODE_OVERHEAD)

    # The mutable(!) queues dict is now empty, i.e. garbage-collected.
    assert len(queues) == 0

    # Truly garbage-collected? Memory freed?
    assert all([ref() is None for ref in refs])


# TODO: also add tests for the depletion of the workers pools on cancellation (+timing)
