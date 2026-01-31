import asyncio

import kopf


async def test_timer_regular_interval(
        resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    trigger = asyncio.Condition()

    @kopf.timer(*resource, id='fn', interval=1.0, sharp=False)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with trigger:
            trigger.notify_all()
        await asyncio.sleep(0.3)  # simulate a slow operation

    await simulate_cycle({})
    async with trigger:
        await trigger.wait()
        await trigger.wait()

    assert dummy.mock.call_count == 2
    assert looptime == 1.3


async def test_timer_sharp_interval(
        resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    trigger = asyncio.Condition()

    @kopf.timer(*resource, id='fn', interval=1.0, sharp=True)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with trigger:
            trigger.notify_all()
        await asyncio.sleep(0.3)  # simulate a slow operation

    await simulate_cycle({})
    async with trigger:
        await trigger.wait_for(lambda: dummy.mock.call_count >= 2)

    assert dummy.mock.call_count == 2
    assert looptime == 1  # not 1.3!
