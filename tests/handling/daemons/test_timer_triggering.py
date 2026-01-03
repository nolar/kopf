import asyncio
import logging

import kopf


async def test_timer_is_spawned_at_least_once(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, looptime):
    caplog.set_level(logging.DEBUG)
    trigger = asyncio.Condition()

    @kopf.timer(*resource, id='fn', interval=1.0)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with trigger:
            trigger.notify_all()

    await simulate_cycle({})
    async with trigger:
        await trigger.wait()
        await trigger.wait()

    assert looptime == 1
    assert dummy.mock.call_count == 2


async def test_timer_initial_delay_obeyed(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, looptime):
    caplog.set_level(logging.DEBUG)
    trigger = asyncio.Condition()

    @kopf.timer(*resource, id='fn', initial_delay=5.0, interval=1.0)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with trigger:
            trigger.notify_all()

    await simulate_cycle({})
    async with trigger:
        await trigger.wait()
        await trigger.wait()

    assert looptime == 6
    assert dummy.mock.call_count == 2
