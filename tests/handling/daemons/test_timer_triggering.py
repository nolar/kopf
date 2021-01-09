import logging

import kopf


async def test_timer_is_spawned_at_least_once(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', interval=1.0)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle

    await simulate_cycle({})
    await dummy.steps['called'].wait()

    assert dummy.mock.call_count == 1
    assert dummy.kwargs['retry'] == 0
    assert k8s_mocked.sleep_or_wait.call_count == 1
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 1.0

    await dummy.wait_for_daemon_done()


async def test_timer_initial_delay_obeyed(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', initial_delay=5.0, interval=1.0)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle

    await simulate_cycle({})
    await dummy.steps['called'].wait()

    assert dummy.mock.call_count == 1
    assert dummy.kwargs['retry'] == 0
    assert k8s_mocked.sleep_or_wait.call_count == 2
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 5.0
    assert k8s_mocked.sleep_or_wait.call_args_list[1][0][0] == 1.0

    await dummy.wait_for_daemon_done()
