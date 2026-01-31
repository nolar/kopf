import asyncio
import contextlib

import pytest

import kopf


async def test_daemon_exits_gracefully_and_instantly_on_resource_deletion(
        settings, resource, dummy, simulate_cycle,
        looptime, assert_logs, k8s_mocked, mocker):
    called = asyncio.Condition()

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn')
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with called:
            called.notify_all()
        await kwargs['stopped'].wait()

    # 0th cycle: trigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    async with called:
        await called.wait()

    # 1st stage: trigger termination due to resource deletion.
    mocker.resetall()
    event_object.setdefault('metadata', {})
    event_object['metadata'] |= {'deletionTimestamp': '...'}
    await simulate_cycle(event_object)

    # Check that the daemon has exited near-instantly, with no delays.
    await dummy.wait_for_daemon_done()

    assert looptime == 0
    assert k8s_mocked.patch.call_count == 1
    assert k8s_mocked.patch.call_args_list[0][1]['payload']['metadata']['finalizers'] == []


async def test_daemon_exits_gracefully_and_instantly_on_operator_exiting(
        settings, resource, dummy, simulate_cycle, background_daemon_killer,
        looptime, assert_logs, k8s_mocked, mocker):
    called = asyncio.Condition()

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn')
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with called:
            called.notify_all()
        await kwargs['stopped'].wait()

    # 0th cycle: trigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    async with called:
        await called.wait()

    # 1st stage: trigger termination due to operator exiting.
    mocker.resetall()
    background_daemon_killer.cancel()

    # Check that the daemon has exited near-instantly, with no delays.
    await dummy.wait_for_daemon_done()

    assert looptime == 0
    assert k8s_mocked.patch.call_count == 0

    # To prevent double-cancelling of the scheduler's system tasks in the fixture, let them finish:
    with contextlib.suppress(asyncio.CancelledError):
        await background_daemon_killer


@pytest.mark.usefixtures('background_daemon_killer')
async def test_daemon_exits_gracefully_and_instantly_on_operator_pausing(
        settings, memories, resource, dummy, simulate_cycle, conflicts_found,
        looptime, assert_logs, k8s_mocked, mocker):
    called = asyncio.Condition()

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn')
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with called:
            called.notify_all()
        await kwargs['stopped'].wait()

    # 0th cycle: trigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    async with called:
        await called.wait()

    # 1st stage: trigger termination due to the operator's pause.
    mocker.resetall()
    await conflicts_found.turn_to(True)

    # Check that the daemon has exited near-instantly, with no delays.
    await dummy.wait_for_daemon_done()
    assert looptime == 0

    # There is no way to test for re-spawning here: it is done by watch-events,
    # which are tested by the paused operators elsewhere (test_daemon_spawning.py).
    # We only test that it is capable for respawning (not forever-stopped):
    memory = await memories.recall(event_object)
    assert not memory.daemons_memory.forever_stopped


async def test_daemon_exits_instantly_on_cancellation_with_backoff(
        settings, resource, dummy, simulate_cycle,
        looptime, assert_logs, k8s_mocked, mocker):
    called = asyncio.Condition()

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn', cancellation_backoff=1.23, cancellation_timeout=10)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with called:
            called.notify_all()
        await asyncio.Event().wait()  # this one is cancelled.

    # Trigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    async with called:
        await called.wait()

    # 1st stage: trigger termination due to resource deletion. Wait for backoff.
    mocker.resetall()
    event_object.setdefault('metadata', {})
    event_object['metadata'] |= {'deletionTimestamp': '...'}
    await simulate_cycle(event_object)

    assert looptime == 1.23  # i.e. the slept through the whole backoff time
    assert k8s_mocked.patch.call_count == 1
    assert k8s_mocked.patch.call_args_list[0][1]['payload']['status']['kopf']['dummy']

    # 2nd cycle: cancelling after the backoff is reached. Wait for cancellation timeout.
    mocker.resetall()
    await simulate_cycle(event_object)

    assert looptime == 1.23  # i.e. no additional sleeps happened
    assert k8s_mocked.patch.call_count == 1
    assert k8s_mocked.patch.call_args_list[0][1]['payload']['metadata']['finalizers'] == []

    # Cleanup.
    await dummy.wait_for_daemon_done()


async def test_daemon_exits_slowly_on_cancellation_with_backoff(
        settings, resource, dummy, simulate_cycle,
        looptime, assert_logs, k8s_mocked, mocker):
    called = asyncio.Condition()
    finish = asyncio.Condition()

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn', cancellation_backoff=1.23, cancellation_timeout=4.56)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with called:
            called.notify_all()
        try:
            await asyncio.Event().wait()  # this one is cancelled.
        except asyncio.CancelledError:
            async with finish:
                await finish.wait()  # simulated slow (non-instant) exiting.

    # Trigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    async with called:
        await called.wait()

    # 1st stage: trigger termination due to resource deletion. Wait for backoff.
    mocker.resetall()
    event_object.setdefault('metadata', {})
    event_object['metadata'] |= {'deletionTimestamp': '...'}
    await simulate_cycle(event_object)

    assert looptime == 1.23
    assert k8s_mocked.patch.call_count == 1
    assert k8s_mocked.patch.call_args_list[0][1]['payload']['status']['kopf']['dummy']

    # 2nd cycle: cancelling after the backoff is reached. Wait for cancellation timeout.
    mocker.resetall()
    await simulate_cycle(event_object)

    assert looptime == 1.23 + 4.56  # i.e. it really spent all the timeout
    assert k8s_mocked.patch.call_count == 1
    assert k8s_mocked.patch.call_args_list[0][1]['payload']['status']['kopf']['dummy']

    # 3rd cycle: the daemon has exited, the resource should be unblocked from actual deletion.
    mocker.resetall()
    async with finish:
        finish.notify_all()
    await asyncio.sleep(0)  # let the daemon to exit and all the routines to trigger
    await simulate_cycle(event_object)
    await dummy.wait_for_daemon_done()

    assert looptime == 1.23 + 4.56  # i.e. not additional sleeps happened
    assert k8s_mocked.patch.call_count == 1
    assert k8s_mocked.patch.call_args_list[0][1]['payload']['metadata']['finalizers'] == []


async def test_daemon_is_abandoned_due_to_cancellation_timeout_reached(
        settings, resource, dummy, simulate_cycle,
        looptime, assert_logs, k8s_mocked, mocker):
    called = asyncio.Condition()
    finish = asyncio.Condition()

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn', cancellation_timeout=4.56)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with called:
            called.notify_all()
        try:
            async with finish:
                await finish.wait()  # this one is cancelled.
        except asyncio.CancelledError:
            async with finish:
                await finish.wait()  # simulated disobedience to be cancelled.

    # 0th cycle:tTrigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    async with called:
        await called.wait()

    # 1st stage: trigger termination due to resource deletion. Wait for backoff.
    mocker.resetall()
    event_object.setdefault('metadata', {})
    event_object['metadata'] |= {'deletionTimestamp': '...'}
    await simulate_cycle(event_object)

    assert looptime == 4.56
    assert k8s_mocked.patch.call_count == 1
    assert k8s_mocked.patch.call_args_list[0][1]['payload']['status']['kopf']['dummy']

    # 2rd cycle: the daemon has exited, the resource should be unblocked from actual deletion.
    mocker.resetall()
    await asyncio.sleep(1000)  # unnecessary, but let's fast-forward time just in case
    with pytest.warns(ResourceWarning, match=r"Daemon .+ did not exit in time"):
        await simulate_cycle(event_object)

    assert looptime == 1000 + 4.56
    assert k8s_mocked.patch.call_count == 1
    assert k8s_mocked.patch.call_args_list[0][1]['payload']['metadata']['finalizers'] == []
    assert_logs(["Daemon 'fn' did not exit in time. Leaving it orphaned."])

    # Cleanup.
    async with finish:
        finish.notify_all()
    await dummy.wait_for_daemon_done()
