import asyncio
import contextlib

from kopf._cogs.aiokits.aiotoggles import Toggle, ToggleSet
from kopf._cogs.clients.watching import Bookmark
from kopf._core.reactor import queueing
from kopf._core.reactor.queueing import ProcessingMode, watcher


async def test_the_overall_scenario_of_operator_blocking_for_preindexing_and_postprocessing(
        resource, settings, kmock, namespace, processor, looptime):
    """
    Duplicates all the individual tests below, but covers the actual realistic scenario end-to-end.
    """

    # Simulate time-distributed requests & events for proper timing of the state changes.
    item1 = {'metadata': {'uid': 'uid1'}}
    item2 = {'metadata': {'uid': 'uid2'}}
    item3 = {'metadata': {'uid': 'uid3'}}
    kmock.resources[resource] = {}
    kmock['list', :1] << (lambda: asyncio.sleep(5)) << {'Content-Type': 'application/json'} << {'items': [item1]}
    kmock['watch', :1] << (
        {'type': 'ADDED', 'object': item2},  # arrives instantly after the listing
        lambda: asyncio.sleep(5),
        {'type': 'ADDED', 'object': item3},  # arrives delayed
        {'type': 'ERROR', 'object': {'code': 410}},
    )

    operator_indexed = ToggleSet(all)
    unrelated_toggle = await operator_indexed.make_toggle()  # other resource kinds are not deleted
    resource_indexed = await operator_indexed.make_toggle()
    watcher_task = asyncio.create_task(watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=processor,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,  # as if index handlers defined
    ))

    # T=1, the listing is initiated but waits on a slow request (ends at t=5).
    await asyncio.sleep(1)  # anything 0.1..4.9 works
    assert looptime == 1
    assert set(operator_indexed) == {resource_indexed, unrelated_toggle}
    assert operator_indexed.is_off()
    assert processor.call_count == 0

    # T=6, the listing comes in at t=5, so the indexing is done by t=6.
    await asyncio.sleep(5)  # must end at t=5.1 or later
    assert looptime == 6
    assert set(operator_indexed) == {unrelated_toggle}
    assert operator_indexed.is_off()
    assert processor.call_count == 1
    assert processor.call_args_list[0].kwargs['processing_mode'] == ProcessingMode.INDEXING_ONLY
    assert processor.call_args_list[0].kwargs['raw_event']['object'] == item1

    # T=6, mark the global operator readiness, but check it only at t=7.
    # This processes the accumulated initial events in the process-only mode (no indexing).
    # Then the immediate watch-stream event arrives, processed normally (index+process).
    await operator_indexed.drop_toggle(unrelated_toggle)
    await asyncio.sleep(1)  # just non-zero to give it time
    assert looptime == 7
    assert set(operator_indexed) == set()
    assert operator_indexed.is_on()
    assert processor.call_count == 3
    assert processor.call_args_list[1].kwargs['processing_mode'] == ProcessingMode.HANDLING_ONLY
    assert processor.call_args_list[1].kwargs['raw_event']['object'] == item1
    assert processor.call_args_list[2].kwargs['processing_mode'] is None
    assert processor.call_args_list[2].kwargs['raw_event']['object'] == item2

    # The 2nd watch-event arrives 5s after the global operator readiness & the 1st event (t=6+5=11).
    # Fast-forward some extra till t=12 for the check.
    await asyncio.sleep(5)  # must end at t=11.1 or later
    assert looptime == 12
    assert set(operator_indexed) == set()
    assert operator_indexed.is_on()
    assert processor.call_count == 4
    assert processor.call_args_list[3].kwargs['processing_mode'] is None
    assert processor.call_args_list[3].kwargs['raw_event']['object'] == item3

    # Clean up after the test. The watcher never ends on its own, it retries forever.
    # todo: make a janitor or garbage bin to throw all such stuff there and forget about it.
    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher_task


async def test_initial_events_are_not_replayed_while_operator_is_paused(
        monkeypatch, resource, settings, namespace, processor, looptime):
    item = {'metadata': {'uid': 'uid1'}}
    stale = {'metadata': {'uid': 'stale'}}
    operator_indexed = ToggleSet(all)
    resource_indexed = await operator_indexed.make_toggle()
    operator_paused = ToggleSet(any)
    keepalive_failed = await operator_paused.make_toggle(False)

    async def stream(**kwargs):
        yield {'type': None, 'object': stale}
        yield Bookmark.RESET
        yield {'type': None, 'object': item}
        await keepalive_failed.turn_to(True)
        yield Bookmark.LISTED
        await asyncio.Event().wait()

    monkeypatch.setattr(queueing.watching, 'infinite_watch', stream)
    watcher_task = asyncio.create_task(watcher(
        namespace=namespace,
        resource=resource,
        settings=settings,
        processor=processor,
        operator_paused=operator_paused,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,
    ))
    try:
        await asyncio.sleep(10)  # long enough for the replay to happen if it were not paused

        assert keepalive_failed.is_on()
        assert processor.call_count == 1
        assert processor.call_args.kwargs['processing_mode'] == ProcessingMode.INDEXING_ONLY
        assert processor.call_args.kwargs['raw_event']['object'] == item

        await keepalive_failed.turn_to(False)
        await asyncio.sleep(1)

        assert looptime == 11
        assert processor.call_count == 2
        assert processor.call_args.kwargs['processing_mode'] == ProcessingMode.HANDLING_ONLY
        assert processor.call_args.kwargs['raw_event']['object'] == item
    finally:
        watcher_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher_task


async def test_absent_indexes_cause_no_processing_before_readiness(
        resource, settings, kmock, namespace, processor):
    """
    In plain English: without an index, no processing calls at all, neither indexing nor handling.
    The regular streaming is blocked by an unrelated toggle present in the set.
    """
    kmock.resources[resource] = {}
    kmock['list', :1] << {'items': [{'metadata': {'uid': 'uid1'}}]}
    kmock['watch', :1] << ({'type': 'ERROR', 'object': {'code': 410}},)

    operator_indexed = ToggleSet(all)
    unrelated_toggle = await operator_indexed.make_toggle()  # other resource kinds are not deleted
    watcher_task = asyncio.create_task(watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=processor,
        operator_indexed=operator_indexed,
        resource_indexed=None,  # as if no index handlers defined
    ))

    await asyncio.sleep(1)
    assert set(operator_indexed) == {unrelated_toggle}
    assert processor.call_count == 0

    # Clean up after the test. The watcher never ends on its own, it retries forever.
    # todo: make a janitor or garbage bin to throw all such stuff there and forget about it.
    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher_task


async def test_stale_toggle_causes_no_processing_before_readiness(
        resource, settings, kmock, namespace, processor):
    """
    In plain English: with an index present but indexing finished earlier,
    make no processing calls at all, neither indexing nor handling.
    This happens when the watcher was respawned after unexpected graceful exit.
    The regular streaming is blocked by an unrelated toggle present in the set.
    """
    kmock.resources[resource] = {}
    kmock['list', :1] << {'items': [{'metadata': {'uid': 'uid1'}}]}
    kmock['watch', :1] << ({'type': 'ERROR', 'object': {'code': 410}},)

    operator_indexed = ToggleSet(all)
    resource_indexed = Toggle()  # but not added to (or dropped from) the toggle-set!
    unrelated_toggle = await operator_indexed.make_toggle()  # other resource kinds are not deleted
    watcher_task = asyncio.create_task(watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=processor,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,  # as if index handlers defined
    ))

    await asyncio.sleep(1)
    assert set(operator_indexed) == {unrelated_toggle}
    assert processor.call_count == 0

    # Clean up after the test. The watcher never ends on its own, it retries forever.
    # todo: make a janitor or garbage bin to throw all such stuff there and forget about it.
    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher_task


async def test_present_indexes_cause_only_indexing_before_readiness(
        resource, settings, kmock, namespace, processor):
    """
    In plain English: with an index, the only call is for indexing, no regular handling.
    The regular streaming is blocked by an unrelated toggle present in the set.
    """
    kmock.resources[resource] = {}
    kmock['list', :1] << {'items': [{'metadata': {'uid': 'uid1'}}]}
    kmock['watch', :1] << ({'type': 'ERROR', 'object': {'code': 410}},)

    operator_indexed = ToggleSet(all)
    unrelated_toggle = await operator_indexed.make_toggle()  # other resource kinds are not deleted
    resource_indexed = await operator_indexed.make_toggle()
    watcher_task = asyncio.create_task(watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=processor,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,  # as if index handlers defined
    ))

    await asyncio.sleep(1)
    assert set(operator_indexed) == {unrelated_toggle}
    assert processor.call_count == 1
    assert processor.call_args_list[-1].kwargs['processing_mode'] == ProcessingMode.INDEXING_ONLY

    # Clean up after the test. The watcher never ends on its own, it retries forever.
    # todo: make a janitor or garbage bin to throw all such stuff there and forget about it.
    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher_task


async def test_present_indexes_cause_only_handling_after_readiness(
        resource, settings, kmock, namespace, processor):
    """
    In plain English: with an index, the 2nd call does not include indexing, strictly handling.
    """
    kmock.resources[resource] = {}
    kmock['list', :1] << {'items': [{'metadata': {'uid': 'uid1'}}]}
    kmock['watch', :1] << ({'type': 'ERROR', 'object': {'code': 410}},)

    operator_indexed = ToggleSet(all)
    resource_indexed = await operator_indexed.make_toggle()
    watcher_task = asyncio.create_task(watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=processor,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,  # as if index handlers defined
    ))

    await asyncio.sleep(1)
    assert processor.call_count == 2
    assert processor.call_args_list[-1].kwargs['processing_mode'] == ProcessingMode.HANDLING_ONLY

    # Clean up after the test. The watcher never ends on its own, it retries forever.
    # todo: make a janitor or garbage bin to throw all such stuff there and forget about it.
    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher_task


async def test_present_indexes_cause_normal_processing_for_streamed_events(
        resource, settings, kmock, namespace, processor):
    """
    In plain English: with an index, the only call does both indexing and handling.
    """
    kmock.resources[resource] = {}
    kmock['list', :1] << {'items': []}
    kmock['watch', :1] << (
        {'type': 'ADDED', 'object': {'metadata': {'uid': 'uid1'}}},
        {'type': 'ERROR', 'object': {'code': 410}},
    )

    operator_indexed = ToggleSet(all)
    resource_indexed = await operator_indexed.make_toggle()
    watcher_task = asyncio.create_task(watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=processor,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,  # as if index handlers defined
    ))

    await asyncio.sleep(1)
    assert processor.call_count == 1
    assert processor.call_args_list[-1].kwargs['processing_mode'] is None

    # Clean up after the test. The watcher never ends on its own, it retries forever.
    # todo: make a janitor or garbage bin to throw all such stuff there and forget about it.
    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher_task


async def test_workers_depleted_after_indexing_before_buffer_handling(
        resource, settings, kmock, namespace, processor, worker_pool_spy):
    """
    In plain English: when we arrive to phase 2 (buffer handling), have no workers left.
    The regular streaming is blocked by an unrelated toggle present in the set.
    """
    kmock.resources[resource] = {}
    kmock['list', :1] << {'items': [{'metadata': {'uid': 'uid1'}}]}
    kmock['watch', :1] << (
        lambda: asyncio.sleep(5),
        {'type': 'ERROR', 'object': {'code': 410}},
    )

    settings.queueing.idle_timeout = 100  # our workers must be non-idling at all

    operator_indexed = ToggleSet(all)
    unrelated_toggle = await operator_indexed.make_toggle()  # other resource kinds are not deleted
    resource_indexed = await operator_indexed.make_toggle()
    watcher_task = asyncio.create_task(watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=processor,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,  # as if index handlers defined
    ))

    await asyncio.sleep(1)
    assert set(operator_indexed) == {unrelated_toggle}  # we are blocked, a precondition for phase 2
    assert not worker_pool_spy.spy_return_list[0].streams  # no workers, as intended

    # Clean up after the test. The watcher never ends on its own, it retries forever.
    # todo: make a janitor or garbage bin to throw all such stuff there and forget about it.
    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher_task


async def test_live_events_queued_behind_buffered_ones_are_processed_in_full(
        resource, settings, kmock, namespace, processor, looptime):
    """
    In plain English: a live event for an object still in its buffered handling is not delayed
    until all buffered handling is over, and is processed in full (including indexing).
    """
    item_v1 = {'metadata': {'uid': 'uid1', 'resourceVersion': 'rv1'}}
    item_v2 = {'metadata': {'uid': 'uid1', 'resourceVersion': 'rv2'}}
    kmock.resources[resource] = {}
    kmock['list', :1] << {'items': [item_v1]}
    kmock['watch', :1] << (
        lambda: asyncio.sleep(1),
        {'type': 'MODIFIED', 'object': item_v2},  # arrives at t=1, amid the buffered handling
        lambda: asyncio.sleep(50),  # below the inactivity timeout, so no reconnects
        {'type': 'ERROR', 'object': {'code': 410}},
    )

    # Make the buffered handling slow (t=0..5), so that the live event queues behind it.
    async def slow_buffered_handling(*, processing_mode, **_):
        if processing_mode == ProcessingMode.HANDLING_ONLY:
            await asyncio.sleep(5)

    processor.side_effect = slow_buffered_handling

    operator_indexed = ToggleSet(all)
    resource_indexed = await operator_indexed.make_toggle()
    watcher_task = asyncio.create_task(watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        processor=processor,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,  # as if index handlers defined
    ))

    # T=4, the buffered handling is still ongoing; the live event (arrived at t=1) is queued.
    await asyncio.sleep(4)
    assert looptime == 4
    assert processor.call_count == 2
    assert processor.call_args_list[0].kwargs['processing_mode'] == ProcessingMode.INDEXING_ONLY
    assert processor.call_args_list[0].kwargs['raw_event']['object'] == item_v1
    assert processor.call_args_list[1].kwargs['processing_mode'] == ProcessingMode.HANDLING_ONLY
    assert processor.call_args_list[1].kwargs['raw_event']['object'] == item_v1

    # T=5.5, the buffered handling is over at t=5, the queued live event is processed right after.
    # If the stream were read only after all buffered handling, the event would arrive at t=6.
    await asyncio.sleep(1.5)
    assert looptime == 5.5
    assert processor.call_count == 3
    assert processor.call_args_list[2].kwargs['processing_mode'] is None  # indexing included
    assert processor.call_args_list[2].kwargs['raw_event']['object'] == item_v2

    # Clean up after the test. The watcher never ends on its own, it retries forever.
    # todo: make a janitor or garbage bin to throw all such stuff there and forget about it.
    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher_task
