import asyncio

import kopf


async def test_daemon_is_spawned_at_least_once(
        resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
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
        resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    executed = asyncio.Event()

    @kopf.daemon(*resource, id='fn', initial_delay=5.0)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        executed.set()

    await simulate_cycle({})
    await executed.wait()

    assert looptime == 5.0


async def test_daemon_initial_delay_callable_obeyed(
        resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    executed = asyncio.Event()

    def get_delay(body, **_):
        return body.get('spec', {}).get('delay', 0.0)

    @kopf.daemon(*resource, id='fn', initial_delay=get_delay)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        executed.set()

    await simulate_cycle({'spec': {'delay': 7.0}})
    await executed.wait()

    assert looptime == 7.0
    assert dummy.mock.call_count == 1
