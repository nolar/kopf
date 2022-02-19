import logging

import kopf

import asyncio


async def test_timer_is_spawned_at_least_once(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', interval=1.0)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        kwargs['stopped']._setter.set()  # to exit the cycle

    await simulate_cycle({})
    await dummy.steps['called'].wait()

    assert dummy.mock.call_count == 1
    assert dummy.kwargs['retry'] == 0
    assert k8s_mocked.sleep.call_count == 1
    assert k8s_mocked.sleep.call_args_list[0][0][0] == 1.0

    await dummy.wait_for_daemon_done()


async def test_timer_stopped_on_deletion_event_nofinalizer(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', interval=1.0, requires_finalizer=False)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs

        if dummy.mock.call_count >= 2:
            dummy.steps['called'].set()
            kwargs['stopped']._setter.set()  # to exit the cycle

    await simulate_cycle({})
    await dummy.steps['called'].wait()

    assert dummy.mock.call_count == 2
    assert dummy.kwargs['retry'] == 0
    assert k8s_mocked.sleep.call_count == 2

    # Send deleted event, wait for 2 seconds (2x interval time)
    # to ensure no more calls.
    # verify no additional calls to fn via mock.call_count
    await simulate_cycle({}, raw_event_type='DELETED')
    await asyncio.sleep(2.0)
    assert dummy.mock.call_count == 2
    await dummy.wait_for_daemon_done()


async def test_timer_initial_delay_obeyed(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', initial_delay=5.0, interval=1.0)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        kwargs['stopped']._setter.set()  # to exit the cycle

    await simulate_cycle({})
    await dummy.steps['called'].wait()

    assert dummy.mock.call_count == 1
    assert dummy.kwargs['retry'] == 0
    assert k8s_mocked.sleep.call_count == 2
    assert k8s_mocked.sleep.call_args_list[0][0][0] == 5.0
    assert k8s_mocked.sleep.call_args_list[1][0][0] == 1.0

    await dummy.wait_for_daemon_done()
