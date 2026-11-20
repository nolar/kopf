import asyncio
from itertools import chain, repeat
from unittest import mock

import aiohttp
import pytest

from kopf._cogs.aiokits import aiotoggles
from kopf._cogs.clients import watching
from kopf._cogs.clients.errors import APIForbiddenError, APIServerError, \
                                      APITooManyRequestsError, APIUnauthorizedError
from kopf._core.engines.peering import keepalive


class StopInfiniteCycleException(Exception):
    pass


@pytest.fixture()
def keepalive_failed():
    return aiotoggles.Toggle(False)


async def test_background_task_runs(
        mocker, settings, assert_logs, namespaced_peering_resource, keepalive_failed):
    touch_mock = mocker.patch('kopf._core.engines.peering.touch')

    sleep_mock = mocker.patch('asyncio.sleep')
    # restore the default behavior after exhausting test values.
    # pytest-aiohttp calls asyncio.sleep during teardown, before the mock is removed.
    sleep_mock.side_effect = chain([None, None, StopInfiniteCycleException], repeat(mock.DEFAULT))

    randint_mock = mocker.patch('random.randint')
    randint_mock.side_effect = [50, 55, 60]

    settings.peering.lifetime = 33
    with pytest.raises(StopInfiniteCycleException):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace',
                        keepalive_failed=keepalive_failed)

    assert randint_mock.call_count == 3  # only to be sure that we test the right thing
    assert sleep_mock.call_count == 3
    assert sleep_mock.call_args_list[0].args[0] == 33 * 50 / 100
    assert sleep_mock.call_args_list[1].args[0] == 33 * 55 / 100
    assert sleep_mock.call_args_list[2].args[0] == 33 * 60 / 100

    assert touch_mock.call_count == 4  # 3 updates + 1 clean-up
    assert_logs(prohibited=["patching"])


@pytest.mark.parametrize('exc', [
    aiohttp.ClientConnectionError(),
    aiohttp.ClientPayloadError(),
    TimeoutError(),
    APIServerError("Unavailable.", status=503, headers={}),
    APITooManyRequestsError("Too many requests.", status=429, headers={}),
])
async def test_temporary_errors_are_retried_after_backoff(
        mocker, settings, assert_logs, namespaced_peering_resource, keepalive_failed, exc):
    touch_mock = mocker.patch('kopf._core.engines.peering.touch')
    touch_mock.side_effect = [exc, None, None]  # a failure, a success, the final clean-up

    sleep_mock = mocker.patch('asyncio.sleep')
    sleep_mock.side_effect = chain([None, StopInfiniteCycleException], repeat(mock.DEFAULT))

    randint_mock = mocker.patch('random.randint')
    randint_mock.side_effect = [50]

    settings.peering.lifetime = 33
    settings.watching.reconnect_backoff = 0.5
    with pytest.raises(StopInfiniteCycleException):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace',
                        keepalive_failed=keepalive_failed)

    assert touch_mock.call_count == 3
    assert sleep_mock.call_count == 2
    assert sleep_mock.call_args_list[0].args[0] == 0.5  # the backoff after the failure
    assert sleep_mock.call_args_list[1].args[0] == 33 * 50 / 100
    assert_logs(["Keep-alive .* failed; pausing and retrying in 0.5s"])


async def test_temporary_error_pauses_until_the_keepalive_recovers(
        mocker, settings, namespaced_peering_resource, keepalive_failed):
    exc = APIServerError("Unavailable.", status=503, headers={})
    attempts = 0

    async def touch(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise exc
        if attempts == 2:
            assert keepalive_failed.is_on()

    mocker.patch('kopf._core.engines.peering.touch', side_effect=touch)
    sleep_mock = mocker.patch('asyncio.sleep')
    sleep_mock.side_effect = chain([None, StopInfiniteCycleException], repeat(mock.DEFAULT))
    mocker.patch('random.randint', return_value=50)

    settings.peering.lifetime = 33
    settings.watching.reconnect_backoff = 0.5
    with pytest.raises(StopInfiniteCycleException):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace',
                        keepalive_failed=keepalive_failed)

    assert keepalive_failed.is_off()


async def test_keepalive_keeps_retrying_in_place_past_the_record_expiry(
        mocker, settings, namespaced_peering_resource, keepalive_failed, looptime):
    exc = APIServerError("Unavailable.", status=503, headers={})
    attempts = 0
    recovered = asyncio.Event()

    async def touch(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return  # initial record written at 0s (expires at 10s)
        if attempts <= 5:
            raise exc  # renewals fail past the 10s expiry while staying paused
        if attempts == 6:
            assert keepalive_failed.is_on()
            recovered.set()

    mocker.patch('kopf._core.engines.peering.touch', side_effect=touch)
    mocker.patch('random.randint', return_value=50)

    settings.peering.lifetime = 10
    settings.watching.reconnect_backoff = 2
    task = asyncio.create_task(keepalive(
        settings=settings, identity='id',
        resource=namespaced_peering_resource, namespace='namespace',
        keepalive_failed=keepalive_failed))
    try:
        await asyncio.wait_for(recovered.wait(), timeout=100)
        await asyncio.sleep(0)
        assert not task.done()
        assert looptime > 10  # well past the initial record expiry
        assert keepalive_failed.is_off()
    finally:
        task.cancel()
        await asyncio.wait([task])


async def test_hanging_keepalive_pauses_before_the_last_record_expires_and_recovers(
        mocker, settings, namespaced_peering_resource, keepalive_failed, looptime):
    attempts = 0
    paused_at: float | None = None
    recovered = asyncio.Event()

    async def touch(**kwargs):
        nonlocal attempts, paused_at
        attempts += 1
        if attempts == 2:
            try:
                await asyncio.Event().wait()  # e.g. the internal retries of the API request
            except asyncio.CancelledError:
                paused_at = asyncio.get_running_loop().time()
                raise
        if attempts == 3:
            assert keepalive_failed.is_on()
            recovered.set()

    mocker.patch('kopf._core.engines.peering.touch', side_effect=touch)
    mocker.patch('random.randint', return_value=50)

    settings.peering.lifetime = 60
    settings.watching.reconnect_backoff = 5
    task = asyncio.create_task(keepalive(
        settings=settings, identity='id',
        resource=namespaced_peering_resource, namespace='namespace',
        keepalive_failed=keepalive_failed))
    try:
        await asyncio.wait_for(recovered.wait(), timeout=100)
        await asyncio.sleep(0)
        assert paused_at == 60 - 10  # the last record expires at 60s; the safety margin is 10s
        assert looptime == 50 + 5  # retried after the 5s backoff and recovered in place
        assert not task.done()
        assert keepalive_failed.is_off()
    finally:
        task.cancel()
        await asyncio.wait([task])


async def test_hanging_first_keepalive_pauses_before_the_record_would_expire_and_recovers(
        mocker, settings, namespaced_peering_resource, keepalive_failed, looptime):
    attempts = 0
    paused_at: float | None = None
    recovered = asyncio.Event()

    async def touch(**kwargs):
        nonlocal attempts, paused_at
        attempts += 1
        if attempts == 1:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                paused_at = asyncio.get_running_loop().time()
                raise
        if attempts == 2:
            assert keepalive_failed.is_on()
            recovered.set()

    mocker.patch('kopf._core.engines.peering.touch', side_effect=touch)

    settings.peering.lifetime = 60
    settings.watching.reconnect_backoff = 5
    task = asyncio.create_task(keepalive(
        settings=settings, identity='id',
        resource=namespaced_peering_resource, namespace='namespace',
        keepalive_failed=keepalive_failed))
    try:
        await asyncio.wait_for(recovered.wait(), timeout=100)
        await asyncio.sleep(0)
        assert paused_at == 60 - 10
        assert looptime == 50 + 5
        assert not task.done()
        assert keepalive_failed.is_off()
    finally:
        task.cancel()
        await asyncio.wait([task])


async def test_lifetime_changed_at_runtime_applies_from_the_next_renewal(
        mocker, settings, namespaced_peering_resource, keepalive_failed, looptime):
    attempts = 0
    paused_at: float | None = None
    recovered = asyncio.Event()

    async def touch(**kwargs):
        nonlocal attempts, paused_at
        attempts += 1
        if attempts == 1:
            settings.peering.lifetime = 10  # e.g. changed by the operator's own code
        if attempts == 3:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                paused_at = asyncio.get_running_loop().time()
                raise
        if attempts == 4:
            recovered.set()

    mocker.patch('kopf._core.engines.peering.touch', side_effect=touch)
    mocker.patch('random.randint', return_value=50)

    settings.peering.lifetime = 60
    settings.watching.reconnect_backoff = 1
    task = asyncio.create_task(keepalive(
        settings=settings, identity='id',
        resource=namespaced_peering_resource, namespace='namespace',
        keepalive_failed=keepalive_failed))
    try:
        await asyncio.wait_for(recovered.wait(), timeout=100)
        # 0s: a 60s record; renewal at 30s writes a 10s record (valid till 40s), renewal at 35s hangs.
        assert paused_at == 40 - 10 * 0.2
        assert attempts == 4
    finally:
        task.cancel()
        await asyncio.wait([task])


@pytest.mark.parametrize('stream', [
    pytest.param((
        {'type': 'BOOKMARK', 'object': {'metadata': {'resourceVersion': '100'}}},
        lambda: asyncio.sleep(999),
    ), id='streaming'),
    pytest.param((lambda: asyncio.sleep(999),), id='waiting-for-headers'),
])
async def test_keepalive_failure_pauses_and_resumes_the_watch_streams(
        kmock, mocker, settings, resource, namespace, namespaced_peering_resource, looptime,
        stream):
    operator_paused = aiotoggles.ToggleSet(any)
    keepalive_failed = await operator_paused.make_toggle(False, name="keep-alive failed")
    attempts = 0

    async def touch(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 2:  # the first renewal at 30s
            raise APIServerError("Unavailable.", status=503, headers={})

    mocker.patch('kopf._core.engines.peering.touch', side_effect=touch)
    mocker.patch('random.randint', return_value=50)
    settings.peering.lifetime = 60
    settings.watching.reconnect_backoff = 5
    kmock['list', resource, kmock.namespace(namespace)] << {
        'metadata': {'resourceVersion': '100'},
        'items': [],
    }
    kmock['watch', resource, kmock.namespace(namespace)] << stream

    async def consume() -> None:
        async for _ in watching.infinite_watch(settings=settings, resource=resource,
                                               namespace=namespace,
                                               operator_paused=operator_paused):
            pass

    keepalive_task = asyncio.create_task(keepalive(
        settings=settings, identity='id',
        resource=namespaced_peering_resource, namespace='namespace',
        keepalive_failed=keepalive_failed))
    watcher_task = asyncio.create_task(consume())
    try:
        await asyncio.sleep(29)
        assert operator_paused.is_off()
        assert len(kmock['watch', resource]) == 1

        await asyncio.sleep(3)  # 32s: the renewal has failed, the retry is due at 35s
        assert operator_paused.is_on()
        assert len(kmock['watch', resource]) == 1  # the stream is closed, not reopened

        await asyncio.sleep(5)  # 37s: the retry has succeeded at 35s
        assert operator_paused.is_off()
        assert len(kmock['watch', resource]) == 2  # streaming again
    finally:
        keepalive_task.cancel()
        watcher_task.cancel()
        await asyncio.wait([keepalive_task, watcher_task])


@pytest.mark.parametrize('percentage', [50, 60])
@pytest.mark.parametrize('lifetime', [1, 1.5, 2, 3, 10, 60])
async def test_short_lifetimes_renew_before_the_pause_deadline(
        mocker, settings, namespaced_peering_resource, keepalive_failed, looptime,
        lifetime, percentage):
    async def touch(**kwargs):
        await asyncio.sleep(0.01)  # a real request takes time

    touch_mock = mocker.patch('kopf._core.engines.peering.touch', side_effect=touch)
    mocker.patch('random.randint', return_value=percentage)

    settings.peering.lifetime = lifetime
    task = asyncio.create_task(keepalive(
        settings=settings, identity='id',
        resource=namespaced_peering_resource, namespace='namespace',
        keepalive_failed=keepalive_failed))
    await asyncio.sleep(lifetime * 10)

    assert not task.done()  # it has not escalated on a healthy cluster
    assert keepalive_failed.is_off()
    assert touch_mock.call_count >= 10
    task.cancel()
    await asyncio.wait([task])


async def test_fractional_lifetime_uses_the_lifetime_of_the_record(
        mocker, settings, namespaced_peering_resource, keepalive_failed, looptime):
    attempts = 0
    paused_at: float | None = None
    recovered = asyncio.Event()

    async def touch(**kwargs):
        nonlocal attempts, paused_at
        attempts += 1
        if attempts == 2:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                paused_at = asyncio.get_running_loop().time()
                raise
        if attempts == 3:
            recovered.set()

    mocker.patch('kopf._core.engines.peering.touch', side_effect=touch)
    mocker.patch('random.randint', return_value=60)

    settings.peering.lifetime = 2.9  # stored in the record as 2 seconds
    settings.watching.reconnect_backoff = 0.5
    task = asyncio.create_task(keepalive(
        settings=settings, identity='id',
        resource=namespaced_peering_resource, namespace='namespace',
        keepalive_failed=keepalive_failed))
    try:
        await asyncio.wait_for(recovered.wait(), timeout=100)
        assert paused_at == pytest.approx(2 - 2 * 0.2)  # the record expiry minus the safety margin
    finally:
        task.cancel()
        await asyncio.wait([task])


@pytest.mark.parametrize('lifetime', [0, 0.5, -1])
async def test_non_positive_lifetime_is_rejected(
        mocker, settings, namespaced_peering_resource, keepalive_failed, lifetime):
    touch_mock = mocker.patch('kopf._core.engines.peering.touch')

    settings.peering.lifetime = lifetime
    with pytest.raises(ValueError, match=r"settings\.peering\.lifetime"):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace',
                        keepalive_failed=keepalive_failed)

    assert touch_mock.call_count == 0


async def test_error_backoff_grows_up_to_the_limit(
        mocker, settings, namespaced_peering_resource, keepalive_failed):
    exc = APIServerError("Unavailable.", status=503, headers={})
    touch_mock = mocker.patch('kopf._core.engines.peering.touch')
    touch_mock.side_effect = [exc] * 7 + [StopInfiniteCycleException, None]
    sleep_mock = mocker.patch('asyncio.sleep')

    settings.watching.reconnect_backoff = 1
    with pytest.raises(StopInfiniteCycleException):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace',
                        keepalive_failed=keepalive_failed)

    assert [call.args[0] for call in sleep_mock.call_args_list] == [1, 2, 4, 8, 10, 10, 10]


async def test_error_backoff_never_goes_below_the_configured_backoff(
        mocker, settings, namespaced_peering_resource, keepalive_failed):
    exc = APIServerError("Unavailable.", status=503, headers={})
    touch_mock = mocker.patch('kopf._core.engines.peering.touch')
    touch_mock.side_effect = [exc] * 3 + [StopInfiniteCycleException, None]
    sleep_mock = mocker.patch('asyncio.sleep')

    settings.watching.reconnect_backoff = 30
    with pytest.raises(StopInfiniteCycleException):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace',
                        keepalive_failed=keepalive_failed)

    assert [call.args[0] for call in sleep_mock.call_args_list] == [30, 30, 30]


async def test_error_backoff_resets_after_a_success(
        mocker, settings, namespaced_peering_resource, keepalive_failed):
    exc = APIServerError("Unavailable.", status=503, headers={})
    touch_mock = mocker.patch('kopf._core.engines.peering.touch')
    touch_mock.side_effect = [exc, exc, None, exc, exc, StopInfiniteCycleException, None]
    sleep_mock = mocker.patch('asyncio.sleep')
    mocker.patch('random.randint', return_value=50)

    settings.peering.lifetime = 33
    settings.watching.reconnect_backoff = 1
    with pytest.raises(StopInfiniteCycleException):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace',
                        keepalive_failed=keepalive_failed)

    assert [call.args[0] for call in sleep_mock.call_args_list] == [1, 2, 16.5, 1, 2]


@pytest.mark.parametrize('enforce, retry_after, expected', [
    pytest.param(False, 2, 5, id='shorter-ignored'),
    pytest.param(True, 2, 2, id='shorter-enforced'),
    pytest.param(False, 7, 7, id='longer'),
    pytest.param(True, 7, 7, id='longer-enforced'),
    pytest.param(False, 30, 30, id='above-the-limit'),
    pytest.param(True, 0, 5, id='zero-enforced'),
    pytest.param(False, 999999, 300, id='huge-capped'),
    pytest.param(True, 999999, 300, id='huge-enforced-capped'),
])
async def test_retry_after_is_respected(
        mocker, settings, namespaced_peering_resource, keepalive_failed,
        enforce, retry_after, expected):
    exc = APITooManyRequestsError(
        "Too many requests.", status=429, headers={'Retry-After': str(retry_after)})
    touch_mock = mocker.patch('kopf._core.engines.peering.touch')
    touch_mock.side_effect = [exc, StopInfiniteCycleException, None]
    sleep_mock = mocker.patch('asyncio.sleep')

    settings.peering.lifetime = 1000
    settings.watching.reconnect_backoff = 5
    settings.networking.enforce_retry_after = enforce
    with pytest.raises(StopInfiniteCycleException):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace',
                        keepalive_failed=keepalive_failed)

    assert [call.args[0] for call in sleep_mock.call_args_list] == [expected]


# Authentication and authorization errors are not retried in place: the operator must fail,
# so that it is restarted as a whole with fresh credentials & permissions.
@pytest.mark.parametrize('exc', [
    APIUnauthorizedError("Unauthorized.", status=401, headers={}),
    APIForbiddenError("Forbidden.", status=403, headers={}),
    RuntimeError("Unexpected."),
])
async def test_other_errors_escalate(
        mocker, settings, namespaced_peering_resource, keepalive_failed, exc):
    touch_mock = mocker.patch('kopf._core.engines.peering.touch')
    touch_mock.side_effect = [exc, None]  # a failure, the final clean-up
    sleep_mock = mocker.patch('asyncio.sleep')

    with pytest.raises(type(exc)):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace',
                        keepalive_failed=keepalive_failed)

    assert touch_mock.call_count == 2
    assert sleep_mock.call_count == 0
