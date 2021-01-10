import logging

import kopf
from kopf.structs.primitives import DaemonStoppingReason


async def test_running_daemon_is_stopped_when_mismatches(
        resource, dummy, timer, mocker, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn', when=lambda **_: is_matching)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        await kwargs['stopped'].wait()

    # Ensure it is spawned while it is matching. (The same as the spawning tests.)
    mocker.resetall()
    is_matching = True
    await simulate_cycle({})
    await dummy.steps['called'].wait()
    assert dummy.mock.call_count == 1

    # Ensure it is stopped once it stops matching. (The same as the termination tests.)
    mocker.resetall()
    is_matching = False
    await simulate_cycle({})
    with timer:
        await dummy.wait_for_daemon_done()

    assert timer.seconds < 0.01  # near-instantly
    stopped = dummy.kwargs['stopped']
    assert DaemonStoppingReason.FILTERS_MISMATCH in stopped.reason
