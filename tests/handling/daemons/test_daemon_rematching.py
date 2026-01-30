import asyncio

import kopf
from kopf._core.intents.stoppers import DaemonStoppingReason


async def test_running_daemon_is_stopped_when_mismatches(
        resource, dummy, looptime, mocker, assert_logs, k8s_mocked, simulate_cycle):
    executed = asyncio.Event()

    @kopf.daemon(*resource, id='fn', when=lambda **_: is_matching)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        executed.set()
        await kwargs['stopped'].wait()

    # Ensure it is spawned while it is matching. (The same as the spawning tests.)
    mocker.resetall()
    is_matching = True
    await simulate_cycle({})
    await executed.wait()
    assert dummy.mock.call_count == 1

    # Ensure it is stopped once it stops matching. (The same as the termination tests.)
    mocker.resetall()
    is_matching = False
    await simulate_cycle({})
    await dummy.wait_for_daemon_done()

    assert looptime == 0
    stopped = dummy.mock.call_args[1]['stopped']
    assert DaemonStoppingReason.FILTERS_MISMATCH in stopped.reason
