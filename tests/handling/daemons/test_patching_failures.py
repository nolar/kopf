import asyncio

import aiohttp

import kopf


async def test_timer_survives_patching_failure(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    k8s_mocked.patch.side_effect = [aiohttp.ClientConnectionError("boo!"), {}, {}, {}]

    @kopf.timer(*resource, id='fn', interval=1.0)
    async def fn(**kwargs):
        dummy.mock(**kwargs)
        return {'result': 'ok'}

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)
    await asyncio.sleep(10)  # give it enough opportunities to retry

    assert dummy.mock.call_count > 1
    assert_logs([
        "Timer 'fn' failed to patch the resource and will retry in 1s: .*boo!.*",
    ])


async def test_daemon_survives_patching_failure(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    k8s_mocked.patch.side_effect = [aiohttp.ClientConnectionError("boo!"), {}, {}, {}]

    @kopf.daemon(*resource, id='fn', backoff=1.0)
    async def fn(retry, patch, **kwargs):
        dummy.mock(**kwargs)
        patch.status['attempt'] = retry  # so that every attempt has something to patch
        if retry < 2:
            raise kopf.TemporaryError("again", delay=1.0)

    event_object = {'metadata': {'finalizers': [settings.persistence.finalizer]}}
    await simulate_cycle(event_object)
    await asyncio.sleep(10)  # give it enough opportunities to retry

    assert dummy.mock.call_count > 1
    assert_logs([
        "Daemon 'fn' failed to patch the resource and will retry in 1s: .*boo!.*",
    ])
