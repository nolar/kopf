import logging

import kopf
from kopf.reactor.handling import PermanentError, TemporaryError
from kopf.structs.handlers import ErrorsMode


async def test_timer_stopped_on_permanent_error(
        settings, resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', backoff=0.01, interval=1.0)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle
        raise PermanentError("boo!")

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)

    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert dummy.mock.call_count == 1
    assert k8s_mocked.sleep_or_wait.call_count == 1  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 1.0

    assert_logs([
        "Timer 'fn' failed permanently: boo!",
    ], prohibited=[
        "Timer 'fn' succeeded.",
    ])


async def test_timer_stopped_on_arbitrary_errors_with_mode_permanent(
        settings, resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', errors=ErrorsMode.PERMANENT, backoff=0.01, interval=1.0)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle
        raise Exception("boo!")

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)

    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert dummy.mock.call_count == 1
    assert k8s_mocked.sleep_or_wait.call_count == 1  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 1.0

    assert_logs([
        "Timer 'fn' failed with an exception. Will stop.",
    ], prohibited=[
        "Timer 'fn' succeeded.",
    ])


async def test_timer_retried_on_temporary_error(
        settings, resource, dummy, manual_time,
        caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', backoff=1.0, interval=1.0)
    async def fn(retry, **kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        if not retry:
            raise TemporaryError("boo!", delay=1.0)
        else:
            kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle
            dummy.steps['finish'].set()

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)

    await dummy.steps['called'].wait()
    await dummy.steps['finish'].wait()
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count == 2  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == [1.0]  # delays
    assert k8s_mocked.sleep_or_wait.call_args_list[1][0][0] == 1.0  # interval

    assert_logs([
        "Timer 'fn' failed temporarily: boo!",
        "Timer 'fn' succeeded.",
    ])


async def test_timer_retried_on_arbitrary_error_with_mode_temporary(
        settings, resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, manual_time):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', errors=ErrorsMode.TEMPORARY, backoff=1.0, interval=1.0)
    async def fn(retry, **kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        if not retry:
            raise Exception("boo!")
        else:
            kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle
            dummy.steps['finish'].set()

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)

    await dummy.steps['called'].wait()
    await dummy.steps['finish'].wait()
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count == 2  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == [1.0]  # delays
    assert k8s_mocked.sleep_or_wait.call_args_list[1][0][0] == 1.0  # interval

    assert_logs([
        "Timer 'fn' failed with an exception. Will retry.",
        "Timer 'fn' succeeded.",
    ])


async def test_timer_retried_until_retries_limit(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, manual_time):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', retries=3, interval=1.0)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        if dummy.mock.call_count >= 5:
            kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle
        raise TemporaryError("boo!", delay=1.0)

    await simulate_cycle({})
    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count >= 4  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == [1.0]  # delays
    assert k8s_mocked.sleep_or_wait.call_args_list[1][0][0] == [1.0]  # delays
    assert k8s_mocked.sleep_or_wait.call_args_list[2][0][0] == [1.0]  # delays
    assert k8s_mocked.sleep_or_wait.call_args_list[3][0][0] == 1.0  # interval


async def test_timer_retried_until_timeout(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, manual_time):
    caplog.set_level(logging.DEBUG)

    @kopf.timer(*resource, id='fn', timeout=3.0, interval=1.0)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        if dummy.mock.call_count >= 5:
            kwargs['stopped']._stopper.set(reason=kopf.DaemonStoppingReason.NONE)  # to exit the cycle
        raise TemporaryError("boo!", delay=1.0)

    await simulate_cycle({})
    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count >= 4  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == [1.0]  # delays
    assert k8s_mocked.sleep_or_wait.call_args_list[1][0][0] == [1.0]  # delays
    assert k8s_mocked.sleep_or_wait.call_args_list[2][0][0] == [1.0]  # delays
    assert k8s_mocked.sleep_or_wait.call_args_list[3][0][0] == 1.0  # interval
