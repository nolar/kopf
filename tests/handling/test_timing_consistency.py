import asyncio
import datetime

import freezegun

import kopf
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.processing import process_resource_event
from kopf.structs.containers import ResourceMemories
from kopf.structs.ephemera import Memo


async def test_consistent_awakening(registry, settings, resource, k8s_mocked, mocker):
    """
    A special case to ensure that "now" is consistent during the handling.

    Previously, "now" of `handler.awakened` and "now" of `state.delay` were
    different (maybe for less than 1 ms). If the scheduled awakening time was
    unlucky to be between these two points in time, the operator stopped
    reacting on this object until any other events or changes arrive.

    Implementation-wise, the operator neither selected the handlers (because
    it was "1ms too early", as per `handler.awakened`),
    nor did it sleep (because it was "1ms too late", as per `state.delay`),
    nor did it produce even a dummy patch (because zero-sleep meant "no sleep").

    After the fix, zero-sleep produces a dummy patch to trigger the reaction
    cycle after the sleep is over (as if it was an actual zero-time sleep).

    In the test, the time granularity is intentionally that low -- 1 µs.
    The time is anyway frozen and does not progress unless explicitly ticked.

    See also: #284
    """

    # Simulate that the object is scheduled to be awakened between the watch-event and sleep.
    ts0 = datetime.datetime(2019, 12, 30, 10, 56, 43)
    tsA_triggered = "2019-12-30T10:56:42.999999"
    ts0_scheduled = "2019-12-30T10:56:43.000000"
    tsB_delivered = "2019-12-30T10:56:43.000001"

    # A dummy handler: it will not be selected for execution anyway, we just need to have it.
    @kopf.on.create(*resource, id='some-id')
    def handler_fn(**_):
        pass

    # Simulate the ticking of time, so that it goes beyond the scheduled awakening time.
    # Any hook point between handler selection and delay calculation is fine,
    # but State.store() also prevents other status-fields from being added and the patch populated.
    def move_to_tsB(*_, **__):
        frozen_dt.move_to(tsB_delivered)

    state_store = mocker.patch('kopf.storage.states.State.store', side_effect=move_to_tsB)
    body = {'status': {'kopf': {'progress': {'some-id': {'delayed': ts0_scheduled}}}}}

    # Simulate the call as if the event has just arrived on the watch-stream.
    # Another way (same effect): handle_resource_changing_cause() and its result.
    with freezegun.freeze_time(tsA_triggered) as frozen_dt:
        assert datetime.datetime.utcnow() < ts0  # extra precaution
        await process_resource_event(
            lifecycle=kopf.lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            resource=resource,
            indexers=OperatorIndexers(),
            memories=ResourceMemories(),
            memobase=Memo(),
            raw_event={'type': 'ADDED', 'object': body},
            event_queue=asyncio.Queue(),
        )
        assert datetime.datetime.utcnow() > ts0  # extra precaution

    assert state_store.called

    # Without "now"-time consistency, neither sleep() would be called, nor a patch applied.
    # Verify that the patch was actually applied, so that the reaction cycle continues.
    assert k8s_mocked.patch_obj.called
    assert 'dummy' in k8s_mocked.patch_obj.call_args_list[-1][1]['patch']['status']['kopf']
