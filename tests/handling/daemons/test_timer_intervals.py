import logging

import kopf

# TODO: tests for idle=  (more complicated)


async def test_timer_regular_interval(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, frozen_time):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', interval=1.0, sharp=False)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        frozen_time.tick(0.3)
        if dummy.mock.call_count >= 2:
            dummy.steps['finish'].set()
            kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle

    await simulate_cycle({})
    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert dummy.mock.call_count == 2
    assert k8s_mocked.sleep_or_wait.call_count == 2
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 1.0
    assert k8s_mocked.sleep_or_wait.call_args_list[1][0][0] == 1.0


async def test_timer_sharp_interval(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, frozen_time):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', interval=1.0, sharp=True)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        frozen_time.tick(0.3)
        if dummy.mock.call_count >= 2:
            dummy.steps['finish'].set()
            kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle

    await simulate_cycle({})
    await dummy.steps['called'].wait()
    await dummy.steps['finish'].wait()
    await dummy.wait_for_daemon_done()

    assert dummy.mock.call_count == 2
    assert k8s_mocked.sleep_or_wait.call_count == 2
    assert 0.7 <= k8s_mocked.sleep_or_wait.call_args_list[0][0][0] < 0.71
    assert 0.7 <= k8s_mocked.sleep_or_wait.call_args_list[1][0][0] < 0.71
