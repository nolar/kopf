import asyncio
import logging

import pytest

import kopf


async def test_daemon_exits_gracefully_and_instantly_on_termination_request(
        settings, resource, dummy, simulate_cycle,
        caplog, assert_logs, k8s_mocked, frozen_time, mocker, timer):
    caplog.set_level(logging.DEBUG)

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn')
    async def fn(**kwargs):
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        await kwargs['stopped'].wait()

    # 0th cycle: trigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await dummy.steps['called'].wait()

    # 1st stage: trigger termination due to resource deletion.
    mocker.resetall()
    event_object.setdefault('metadata', {}).update({'deletionTimestamp': '...'})
    await simulate_cycle(event_object)

    # Check that the daemon has exited near-instantly, with no delays.
    with timer:
        await dummy.wait_for_daemon_done()

    assert timer.seconds < 0.01  # near-instantly
    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert k8s_mocked.patch_obj.call_args_list[0][1]['patch']['metadata']['finalizers'] == []


@pytest.mark.usefixtures('background_daemon_killer')
async def test_daemon_exits_gracefully_and_instantly_on_operator_pausing(
        settings, memories, resource, dummy, simulate_cycle, conflicts_found,
        caplog, assert_logs, k8s_mocked, frozen_time, mocker, timer):
    caplog.set_level(logging.DEBUG)

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn')
    async def fn(**kwargs):
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        await kwargs['stopped'].wait()

    # 0th cycle: trigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await dummy.steps['called'].wait()

    # 1st stage: trigger termination due to the operator's pause.
    mocker.resetall()
    await conflicts_found.turn_to(True)

    # Check that the daemon has exited near-instantly, with no delays.
    with timer:
        await dummy.wait_for_daemon_done()
    assert timer.seconds < 0.01  # near-instantly

    # There is no way to test for re-spawning here: it is done by watch-events,
    # which are tested by the paused operators elsewhere (test_daemon_spawning.py).
    # We only test that it is capable for respawning (not forever-stopped):
    memory = await memories.recall(event_object)
    assert not memory.forever_stopped


async def test_daemon_exits_instantly_via_cancellation_with_backoff(
        settings, resource, dummy, simulate_cycle,
        caplog, assert_logs, k8s_mocked, frozen_time, mocker):
    caplog.set_level(logging.DEBUG)
    dummy.steps['finish'].set()

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn', cancellation_backoff=5, cancellation_timeout=10)
    async def fn(**kwargs):
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        try:
            await asyncio.Event().wait()  # this one is cancelled.
        except asyncio.CancelledError:
            await dummy.steps['finish'].wait()  # simulated slow (non-instant) exiting.

    # Trigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await dummy.steps['called'].wait()

    # 1st stage: trigger termination due to resource deletion. Wait for backoff.
    mocker.resetall()
    event_object.setdefault('metadata', {}).update({'deletionTimestamp': '...'})
    await simulate_cycle(event_object)

    assert k8s_mocked.sleep_or_wait.call_count == 1
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 5.0
    assert k8s_mocked.patch_obj.call_count == 1
    assert k8s_mocked.patch_obj.call_args_list[0][1]['patch']['status']['kopf']['dummy']

    # 2nd cycle: cancelling after the backoff is reached. Wait for cancellation timeout.
    mocker.resetall()
    frozen_time.tick(5)  # backoff time or slightly above it
    await simulate_cycle(event_object)

    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert k8s_mocked.patch_obj.call_args_list[0][1]['patch']['metadata']['finalizers'] == []

    # Cleanup.
    await dummy.wait_for_daemon_done()


async def test_daemon_exits_slowly_via_cancellation_with_backoff(
        settings, resource, dummy, simulate_cycle,
        caplog, assert_logs, k8s_mocked, frozen_time, mocker):
    caplog.set_level(logging.DEBUG)

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn', cancellation_backoff=5, cancellation_timeout=10)
    async def fn(**kwargs):
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        try:
            await asyncio.Event().wait()  # this one is cancelled.
        except asyncio.CancelledError:
            await dummy.steps['finish'].wait()  # simulated slow (non-instant) exiting.

    # Trigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await dummy.steps['called'].wait()

    # 1st stage: trigger termination due to resource deletion. Wait for backoff.
    mocker.resetall()
    event_object.setdefault('metadata', {}).update({'deletionTimestamp': '...'})
    await simulate_cycle(event_object)

    assert k8s_mocked.sleep_or_wait.call_count == 1
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 5.0
    assert k8s_mocked.patch_obj.call_count == 1
    assert k8s_mocked.patch_obj.call_args_list[0][1]['patch']['status']['kopf']['dummy']

    # 2nd cycle: cancelling after the backoff is reached. Wait for cancellation timeout.
    mocker.resetall()
    frozen_time.tick(5)  # backoff time or slightly above it
    await simulate_cycle(event_object)

    assert k8s_mocked.sleep_or_wait.call_count == 1
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 10.0
    assert k8s_mocked.patch_obj.call_count == 1
    assert k8s_mocked.patch_obj.call_args_list[0][1]['patch']['status']['kopf']['dummy']

    # 3rd cycle: the daemon has exited, the resource should be unblocked from actual deletion.
    mocker.resetall()
    frozen_time.tick(1)  # any time below timeout
    dummy.steps['finish'].set()
    await asyncio.sleep(0)
    await simulate_cycle(event_object)
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert k8s_mocked.patch_obj.call_args_list[0][1]['patch']['metadata']['finalizers'] == []


async def test_daemon_is_abandoned_due_to_cancellation_timeout_reached(
        settings, resource, dummy, simulate_cycle,
        caplog, assert_logs, k8s_mocked, frozen_time, mocker):
    caplog.set_level(logging.DEBUG)

    # A daemon-under-test.
    @kopf.daemon(*resource, id='fn', cancellation_timeout=10)
    async def fn(**kwargs):
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        try:
            await dummy.steps['finish'].wait()  # this one is cancelled.
        except asyncio.CancelledError:
            await dummy.steps['finish'].wait()  # simulated disobedience to be cancelled.

    # 0th cycle:tTrigger spawning and wait until ready. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await dummy.steps['called'].wait()

    # 1st stage: trigger termination due to resource deletion. Wait for backoff.
    mocker.resetall()
    event_object.setdefault('metadata', {}).update({'deletionTimestamp': '...'})
    await simulate_cycle(event_object)

    assert k8s_mocked.sleep_or_wait.call_count == 1
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 10.0
    assert k8s_mocked.patch_obj.call_count == 1
    assert k8s_mocked.patch_obj.call_args_list[0][1]['patch']['status']['kopf']['dummy']

    # 2rd cycle: the daemon has exited, the resource should be unblocked from actual deletion.
    mocker.resetall()
    frozen_time.tick(50)
    with pytest.warns(ResourceWarning, match=r"Daemon .+ did not exit in time"):
        await simulate_cycle(event_object)

    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert k8s_mocked.patch_obj.call_args_list[0][1]['patch']['metadata']['finalizers'] == []
    assert_logs(["Daemon 'fn' did not exit in time. Leaving it orphaned."])

    # Cleanup.
    dummy.steps['finish'].set()
    await dummy.wait_for_daemon_done()
