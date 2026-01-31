import asyncio

import kopf


async def test_timer_is_spawned_at_least_once(
        resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
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
        resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
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


async def test_timer_initial_delay_callable_obeyed(
        resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    trigger = asyncio.Condition()

    def get_delay(body, **_):
        return body.get('spec', {}).get('delay', 0.0)

    @kopf.timer(*resource, id='fn', initial_delay=get_delay, interval=1.0)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with trigger:
            trigger.notify_all()

    await simulate_cycle({'spec': {'delay': 3.0}})
    async with trigger:
        await trigger.wait()
        await trigger.wait()

    assert looptime == 4.0 # 3.0 delay + 1.0 interval
    assert dummy.mock.call_count == 2
