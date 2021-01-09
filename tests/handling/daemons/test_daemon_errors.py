import logging

import kopf
from kopf.reactor.handling import PermanentError, TemporaryError
from kopf.structs.handlers import ErrorsMode


async def test_daemon_stopped_on_permanent_error(
        settings, resource, dummy, manual_time, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn', backoff=0.01)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        raise PermanentError("boo!")

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)

    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert dummy.mock.call_count == 1
    assert k8s_mocked.patch_obj.call_count == 0
    assert k8s_mocked.sleep_or_wait.call_count == 0

    assert_logs([
        "Daemon 'fn' failed permanently: boo!",
        "Daemon 'fn' has exited on its own",
    ], prohibited=[
        "Daemon 'fn' succeeded.",
    ])


async def test_daemon_stopped_on_arbitrary_errors_with_mode_permanent(
        settings, resource, dummy, manual_time, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn', errors=ErrorsMode.PERMANENT, backoff=0.01)
    async def fn(**kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        raise Exception("boo!")

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)

    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert dummy.mock.call_count == 1
    assert k8s_mocked.sleep_or_wait.call_count == 0

    assert_logs([
        "Daemon 'fn' failed with an exception. Will stop.",
        "Daemon 'fn' has exited on its own",
    ], prohibited=[
        "Daemon 'fn' succeeded.",
    ])


async def test_daemon_retried_on_temporary_error(
        registry, settings, resource, dummy, manual_time,
        caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn', backoff=1.0)
    async def fn(retry, **kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        if not retry:
            raise TemporaryError("boo!", delay=1.0)
        else:
            dummy.steps['finish'].set()

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)

    await dummy.steps['called'].wait()
    await dummy.steps['finish'].wait()
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count == 1  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 1.0  # [call#][args/kwargs][arg#]

    assert_logs([
        "Daemon 'fn' failed temporarily: boo!",
        "Daemon 'fn' succeeded.",
        "Daemon 'fn' has exited on its own",
    ])


async def test_daemon_retried_on_arbitrary_error_with_mode_temporary(
        settings, resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, manual_time):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn', errors=ErrorsMode.TEMPORARY, backoff=1.0)
    async def fn(retry, **kwargs):
        dummy.mock()
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        if not retry:
            raise Exception("boo!")
        else:
            dummy.steps['finish'].set()

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)

    await dummy.steps['called'].wait()
    await dummy.steps['finish'].wait()
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count == 1  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 1.0  # [call#][args/kwargs][arg#]

    assert_logs([
        "Daemon 'fn' failed with an exception. Will retry.",
        "Daemon 'fn' succeeded.",
        "Daemon 'fn' has exited on its own",
    ])


async def test_daemon_retried_until_retries_limit(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, manual_time):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn', retries=3)
    async def fn(**kwargs):
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        raise TemporaryError("boo!", delay=1.0)

    await simulate_cycle({})
    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count == 3  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 1.0  # [call#][args/kwargs][arg#]
    assert k8s_mocked.sleep_or_wait.call_args_list[1][0][0] == 1.0  # [call#][args/kwargs][arg#]
    assert k8s_mocked.sleep_or_wait.call_args_list[2][0][0] == 1.0  # [call#][args/kwargs][arg#]


async def test_daemon_retried_until_timeout(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, manual_time):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn', timeout=3.0)
    async def fn(**kwargs):
        dummy.kwargs = kwargs
        dummy.steps['called'].set()
        raise TemporaryError("boo!", delay=1.0)

    await simulate_cycle({})
    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()

    assert k8s_mocked.sleep_or_wait.call_count == 3  # one for each retry
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == 1.0  # [call#][args/kwargs][arg#]
    assert k8s_mocked.sleep_or_wait.call_args_list[1][0][0] == 1.0  # [call#][args/kwargs][arg#]
    assert k8s_mocked.sleep_or_wait.call_args_list[2][0][0] == 1.0  # [call#][args/kwargs][arg#]
