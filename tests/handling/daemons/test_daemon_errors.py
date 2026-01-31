import asyncio

import kopf
from kopf._core.actions.execution import ErrorsMode, PermanentError, TemporaryError


async def test_daemon_stopped_on_permanent_error(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):

    @kopf.daemon(*resource, id='fn', backoff=1.23)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        raise PermanentError("boo!")

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await asyncio.sleep(123)  # give it enough opportunities to misbehave (e.g. restart)

    assert looptime == 123
    assert dummy.mock.call_count == 1

    assert_logs([
        "Daemon 'fn' failed permanently: boo!",
        "Daemon 'fn' has exited on its own",
    ], prohibited=[
        "Daemon 'fn' succeeded.",
    ])


async def test_daemon_stopped_on_arbitrary_errors_with_mode_permanent(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):

    @kopf.daemon(*resource, id='fn', errors=ErrorsMode.PERMANENT, backoff=1.23)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        raise Exception("boo!")

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await asyncio.sleep(123)  # give it enough opportunities to misbehave (e.g. restart)

    assert looptime == 123
    assert dummy.mock.call_count == 1

    assert_logs([
        "Daemon 'fn' failed with an exception and will stop now: boo!",
        "Daemon 'fn' has exited on its own",
    ], prohibited=[
        "Daemon 'fn' succeeded.",
    ])


async def test_daemon_retried_on_temporary_error(
        registry, settings, resource, dummy,
        assert_logs, k8s_mocked, simulate_cycle, looptime):
    finished = asyncio.Event()

    @kopf.daemon(*resource, id='fn', backoff=1.23)
    async def fn(retry, **kwargs):
        dummy.mock(**kwargs)
        if not retry:
            raise TemporaryError("boo!", delay=3.45)
        else:
            finished.set()

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await finished.wait()

    assert looptime == 3.45
    assert_logs([
        "Daemon 'fn' failed temporarily: boo!",
        "Daemon 'fn' succeeded.",
        "Daemon 'fn' has exited on its own",
    ])


async def test_daemon_retried_on_arbitrary_error_with_mode_temporary(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    finished = asyncio.Event()

    @kopf.daemon(*resource, id='fn', errors=ErrorsMode.TEMPORARY, backoff=1.23)
    async def fn(retry, **kwargs):
        dummy.mock(**kwargs)
        if not retry:
            raise Exception("boo!")
        else:
            finished.set()

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await finished.wait()

    assert looptime == 1.23
    assert_logs([
        "Daemon 'fn' failed with an exception and will try again in 1.23 seconds: boo!",
        "Daemon 'fn' succeeded.",
        "Daemon 'fn' has exited on its own",
    ])


async def test_daemon_retried_until_retries_limit(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, looptime):
    trigger = asyncio.Condition()

    @kopf.daemon(*resource, id='fn', retries=3)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with trigger:
            trigger.notify_all()
        raise TemporaryError("boo!", delay=1.23)

    await simulate_cycle({})
    async with trigger:
        await trigger.wait_for(lambda: any("but will" in m for m in caplog.messages))

    assert looptime == 2.46
    assert dummy.mock.call_count == 3


async def test_daemon_retried_until_timeout(
        resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, looptime):
    trigger = asyncio.Condition()

    @kopf.daemon(*resource, id='fn', timeout=4)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with trigger:
            trigger.notify_all()
        raise TemporaryError("boo!", delay=1.23)

    await simulate_cycle({})
    async with trigger:
        await trigger.wait_for(lambda: any("but will" in m for m in caplog.messages))

    assert looptime == 3.69
    assert dummy.mock.call_count == 4
