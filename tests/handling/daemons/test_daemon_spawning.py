import logging

import kopf


async def test_daemon_is_spawned_at_least_once(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn')
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()

    await simulate_cycle({})

    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert dummy.mock.call_count == 1  # not restarted


async def test_daemon_initial_delay_obeyed(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn', initial_delay=1.0)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()

    await simulate_cycle({})

    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count >= 1
    assert k8s_mocked.sleep_or_wait.call_count <= 2  # one optional extra call for sleep(None)
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 1.0  # [call#][args/kwargs][arg#]
