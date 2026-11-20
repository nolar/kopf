"""
Ensure the consistency tracking survives the switch from buffered handling to live streaming.

The real resource processor is used here (not a mock): the watcher, the workers, the causation,
the handlers, and the patching -- all of them are involved, only the API is emulated.
"""
import asyncio
import contextlib
import functools

import kopf
from kopf._cogs.aiokits.aiotoggles import ToggleSet
from kopf._cogs.structs.ephemera import Memo
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.reactor.processing import process_resource_event
from kopf._core.reactor.queueing import watcher


def _merged(body, patch):
    """ Apply a merge-patch to a body, the same way as the API server would do. """
    result = dict(body)
    for key, val in patch.items():
        if val is None:
            result.pop(key, None)
        elif isinstance(val, dict):
            result[key] = _merged(result.get(key) if isinstance(result.get(key), dict) else {}, val)
        else:
            result[key] = val
    return result


async def test_stale_event_after_buffered_handling_does_not_rerun_creation(
        resource, settings, registry, memories, kmock, namespace, mocker, looptime):
    """
    In plain English: an object is listed, then modified by someone else, then patched by us.
    The modification arrives after our patch, but before our patch's own event, so it is stale.
    It must be skipped as inconsistent, even if the buffered handling of other objects is slow.

    The timeline of both objects in the buffered handling and then in the live streaming:

    obj1: --|-------------|-------------|-------------|---------------------------|------->
            t=0           t=1           t=2           t=5                         t=15
            listed,       stale rv12    patched rv13  expectation                 check:
            indexed,      arrives:      arrives:      would expire                created
            created,      inconsistent, consistent,   (but already                once
            patched,      skipped       nothing to do met at t=2)
            rv13 expected

    obj2: --|-------------------------------------------------------|-------------|------->
            t=0                                                     t=10          t=15
            listed,                                                 created,      check:
            indexed,                                                patched       created
            creation                                                rv22          once
            starts (10s)

    If the stream were read only after all the buffered handling is over (t=10), the stale rv12
    would arrive at t=11, after obj1's expectation of rv13 had expired (t=5), so it would be
    deemed consistent, and the creation would be repeated on the stale body.
    """
    loop = asyncio.get_running_loop()
    metadata = {} if namespace is None else {'namespace': namespace}
    obj1_v1 = {'metadata': {'uid': 'uid1', 'name': 'obj1', 'resourceVersion': 'rv11', **metadata}}
    obj2_v1 = {'metadata': {'uid': 'uid2', 'name': 'obj2', 'resourceVersion': 'rv21', **metadata}}
    obj1_v2 = {**obj1_v1,
               'metadata': {**obj1_v1['metadata'], 'resourceVersion': 'rv12'},
               'status': {'phase': 'Running'}}  # modified by someone else, not by us

    # Respond to our patches as the API server would do: apply them on the latest server state.
    # Remember the result to stream it later as the patch's own watch-event.
    patched = {}

    def patch_obj1(req):
        patched['obj1'] = _merged(_merged(obj1_v2, req.data), {'metadata': {'resourceVersion': 'rv13'}})
        return patched['obj1']

    def patch_obj2(req):
        patched['obj2'] = _merged(_merged(obj2_v1, req.data), {'metadata': {'resourceVersion': 'rv22'}})
        return patched['obj2']

    kmock.resources[resource] = {}
    kmock.objects[resource, namespace, 'obj1'] = {}  # suppress 404s in the emulator
    kmock.objects[resource, namespace, 'obj2'] = {}  # suppress 404s in the emulator
    kmock['patch', resource, kmock.name('obj1')] << patch_obj1
    kmock['patch', resource, kmock.name('obj2')] << patch_obj2
    kmock['list', :1] << {'items': [obj1_v1, obj2_v1]}
    kmock['watch', :1] << (
        lambda: asyncio.sleep(1),
        {'type': 'MODIFIED', 'object': obj1_v2},  # t=1: stale, pre-dates our patch of obj1
        lambda: asyncio.sleep(1),
        lambda: {'type': 'MODIFIED', 'object': patched['obj1']},  # t=2: our patch of obj1
        lambda: asyncio.sleep(50),  # below the inactivity timeout, so no reconnects
        {'type': 'ERROR', 'object': {'code': 410}},
    )

    # The slow obj2 keeps the buffered handling busy for longer than the consistency timeout.
    # If the stream were read only after all buffered handling (t=10), obj1's expectation of
    # its patched version would expire by then (t=5), and the stale event would re-create it.
    create_mock = mocker.Mock()

    @kopf.on.create('kopfexamples', id='create_fn')
    async def create_fn(name, **_):
        create_mock(name=name, _time=loop.time())
        if name == 'obj2':
            await asyncio.sleep(10)

    settings.persistence.consistency_timeout = 5
    operator_indexed = ToggleSet(all)
    resource_indexed = await operator_indexed.make_toggle()
    watcher_task = asyncio.create_task(watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,  # as if index handlers defined
        processor=functools.partial(process_resource_event,
                                    lifecycle=kopf.lifecycles.all_at_once,
                                    registry=registry,
                                    settings=settings,
                                    indexers=OperatorIndexers(),
                                    memories=memories,
                                    memobase=Memo(),
                                    event_queue=asyncio.Queue(),
                                    resource=resource),
    ))

    # T=15, well after obj2's slow creation (t=10) and the consistency timeout of obj1 (t=5).
    await asyncio.sleep(15)
    assert looptime == 15
    assert not watcher_task.done()  # no unrecoverable errors in the workers

    # Both objects were patched once each, and obj1's patched version did arrive and match.
    assert set(patched) == {'obj1', 'obj2'}
    obj1_memory = await memories.recall(obj1_v1)
    assert obj1_memory.expected_version is None

    # Each object was created exactly once, at the buffered handling; the stale event was skipped.
    assert sorted(call.kwargs['name'] for call in create_mock.call_args_list) == ['obj1', 'obj2']
    assert all(call.kwargs['_time'] == 0 for call in create_mock.call_args_list)

    # Clean up after the test. The watcher never ends on its own, it retries forever.
    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher_task
