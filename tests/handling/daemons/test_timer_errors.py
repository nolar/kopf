import asyncio

import kopf
from kopf._core.actions.execution import ErrorsMode, PermanentError, TemporaryError


async def test_timer_stopped_on_permanent_error(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):

    @kopf.timer(*resource, id='fn', backoff=1.23, interval=999)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        raise PermanentError("boo!")

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)
    await asyncio.sleep(123)  # give it enough opportunities to misbehave (e.g. retry)

    assert looptime == 123  # no intervals used, as there were no retries
    assert dummy.mock.call_count == 1

    assert_logs([
        "Timer 'fn' failed permanently: boo!",
    ], prohibited=[
        "Timer 'fn' succeeded.",
    ])


async def test_timer_stopped_on_arbitrary_errors_with_mode_permanent(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):

    @kopf.timer(*resource, id='fn', errors=ErrorsMode.PERMANENT, backoff=1.23, interval=999)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        raise Exception("boo!")

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)
    await asyncio.sleep(123)  # give it enough opportunities to misbehave (e.g. retry)

    assert looptime == 123  # no intervals used, as there were no retries
    assert dummy.mock.call_count == 1

    assert_logs([
        "Timer 'fn' failed with an exception and will stop now: boo!",
    ], prohibited=[
        "Timer 'fn' succeeded.",
    ])


async def test_timer_retried_on_temporary_error(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    finished = asyncio.Event()

    @kopf.timer(*resource, id='fn', backoff=1.23, interval=2.34)
    async def fn(retry, **kwargs):
        dummy.mock(**kwargs)
        if not retry:
            raise TemporaryError("boo!", delay=3.45)
        else:
            finished.set()

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)
    await finished.wait()

    assert looptime == 3.45
    assert_logs([
        "Timer 'fn' failed temporarily: boo!",
        "Timer 'fn' succeeded.",
    ])


async def test_timer_retried_on_arbitrary_error_with_mode_temporary(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    finished = asyncio.Event()

    @kopf.timer(*resource, id='fn', errors=ErrorsMode.TEMPORARY, backoff=1.23, interval=2.34)
    async def fn(retry, **kwargs):
        dummy.mock(**kwargs)
        if not retry:
            raise Exception("boo!")
        else:
            finished.set()

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)
    await finished.wait()

    assert looptime == 1.23
    assert_logs([
        "Timer 'fn' failed with an exception and will try again in 1.23 seconds: boo!",
        "Timer 'fn' succeeded.",
    ])


async def test_timer_retried_until_retries_limit(
        settings, resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, looptime):
    trigger = asyncio.Condition()

    @kopf.timer(*resource, id='fn', retries=3, interval=2.34)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with trigger:
            trigger.notify_all()
        raise TemporaryError("boo!", delay=3.45)

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)
    async with trigger:
        await trigger.wait_for(lambda: any("but will" in m for m in caplog.messages))

    assert looptime == 6.9  # 2*3.45 -- 2 sleeps between 3 attempts
    assert dummy.mock.call_count == 3


async def test_timer_retried_until_timeout(
        settings, resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle, looptime):
    trigger = asyncio.Condition()

    @kopf.timer(*resource, id='fn', timeout=10.0, interval=1.23)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        async with trigger:
            trigger.notify_all()
        raise TemporaryError("boo!", delay=3.45)

    await simulate_cycle({})
    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)
    async with trigger:
        await trigger.wait_for(lambda: any("but will" in m for m in caplog.messages))

    assert looptime == 6.9  # 2*3.45 -- 2 sleeps between 3 attempts
    assert dummy.mock.call_count == 3
