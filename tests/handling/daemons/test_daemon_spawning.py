import asyncio
import logging

import kopf


async def test_daemon_is_spawned_at_least_once(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, looptime):
    caplog.set_level(logging.DEBUG)
    executed = asyncio.Event()

    @kopf.daemon(*resource, id='fn')
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        executed.set()

    await simulate_cycle({})
    await executed.wait()

    assert looptime == 0
    assert dummy.mock.call_count == 1  # not restarted


async def test_daemon_initial_delay_obeyed(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, looptime):
    caplog.set_level(logging.DEBUG)
    executed = asyncio.Event()

    @kopf.daemon(*resource, id='fn', initial_delay=5.0)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        executed.set()

    await simulate_cycle({})
    await executed.wait()

    assert looptime == 5.0
