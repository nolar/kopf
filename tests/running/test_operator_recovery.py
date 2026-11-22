"""
Whole-operator tests: temporary API errors must not stop the operator early.

These tests run the real operator (``kopf.operator()``) against the simulated API server
and let the server fail the way a real one does. They show what an operator does during
the outage and after it, not what the individual functions return.

The operator keeps running through temporary errors. It stops only when it cannot recover,
such as on authorization errors.
"""
import asyncio
import contextlib
import datetime
import email.utils
import logging

import pytest
from kmock import Response

import kopf
from kopf._cogs.clients import errors, watching
from kopf._cogs.structs.credentials import ConnectionInfo, Vault, VaultKey
from kopf._cogs.structs.references import Resource

EXAMPLES = Resource('kopf.dev', 'v1', 'kopfexamples', namespaced=True)
PEERINGS = Resource('kopf.dev', 'v1', 'clusterkopfpeerings', namespaced=False)


@pytest.fixture()
def vault(kmock):
    return Vault({VaultKey('fixture'): ConnectionInfo(server=str(kmock.url))})


@pytest.fixture(autouse=True)
def api_resources(kmock, settings):
    settings.scanning.disabled = True  # the discovered resources do not change in these tests
    settings.process.ultimate_exiting_timeout = None  # never SIGKILL the test process
    kmock.resources['v1/namespaces'] = {}
    kmock.resources['v1/events'] = {}
    kmock.resources['apiextensions.k8s.io/v1/customresourcedefinitions'] = {}
    kmock.resources['kopf.dev/v1/kopfexamples'] = {}
    kmock.resources['kopf.dev/v1/clusterkopfpeerings'] = {}
    for key in ['v1/namespaces', 'v1/events', 'apiextensions.k8s.io/v1/customresourcedefinitions',
                'kopf.dev/v1/kopfexamples', 'kopf.dev/v1/clusterkopfpeerings']:
        kmock.resources[key].verbs = {'get', 'list', 'watch', 'patch', 'create', 'delete'}
    kmock.resources['kopf.dev/v1/kopfexamples'].kind = 'KopfExample'
    kmock.resources['kopf.dev/v1/kopfexamples'].namespaced = True
    kmock.resources['kopf.dev/v1/clusterkopfpeerings'].kind = 'ClusterKopfPeering'
    kmock.resources['kopf.dev/v1/clusterkopfpeerings'].namespaced = False


@pytest.fixture()
def run_operator(registry, settings, vault):
    """Return an async context manager that runs the operator in the background."""

    @contextlib.asynccontextmanager
    async def running(standalone: bool = True):
        stop_flag = asyncio.Event()
        task = asyncio.create_task(kopf.operator(
            registry=registry, settings=settings, vault=vault,
            standalone=standalone, clusterwide=True, stop_flag=stop_flag))
        try:
            yield task
        finally:
            stop_flag.set()
            await asyncio.wait([task], timeout=30)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await vault.close()

    return running


async def wait_for(condition, *, kmock, timeout: float = 60):
    """Poll for a condition on the virtual clock, so that real I/O gets time to proceed."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        assert len(kmock) < 300, "The operator is hammering the API server."
        assert asyncio.get_running_loop().time() < deadline, "The condition was never met."
        await asyncio.sleep(0.1)


def once(payload, *, times: list[float] | None = None):
    """
    Serve the payload to the first matching request only.

    Later requests go to the next reaction, i.e. to the simulated API server itself.
    The loop time of every request is appended to ``times`` (if given).
    """
    served = False

    def serve():
        nonlocal served
        if times is not None:
            times.append(asyncio.get_running_loop().time())
        if served:
            return StopIteration  # kmock: "this reaction does not apply", try the next one
        served = True
        return payload

    return serve


HTTP_DATE = object()  # a marker: the Retry-After header is an HTTP date, built in the test


def http_date(seconds_ahead: int) -> str:
    when = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=seconds_ahead)
    return email.utils.format_datetime(when, usegmt=True)


def outage(*, begin: float = 0, end: float = float('inf'), status: int = 503,
           only=None, bodies=None):
    """
    Fail the matching requests while the loop time is within the window.

    The bodies of all requests that reach this reaction are appended to ``bodies`` (if given).
    """
    def serve(request):
        if bodies is not None:
            bodies.append(request.data)
        now = asyncio.get_running_loop().time()
        if begin <= now < end and (only is None or only(request)):
            return Response(status=status, payload={'kind': 'Status', 'code': status,
                                                    'reason': 'Temporary', 'message': 'Outage.'})
        raise NotImplementedError  # kmock: skip this reaction now, but keep it for later requests
    return serve


async def create_peering(kmock) -> None:
    await kmock.post('/apis/kopf.dev/v1/clusterkopfpeerings',
                     json={'metadata': {'name': 'default'}})


async def sleep_until(when: float) -> None:
    await asyncio.sleep(when - asyncio.get_running_loop().time())


@pytest.mark.parametrize('code', [404, 429, 503])
async def test_watch_continues_after_an_error_event_in_the_stream(
        kmock, registry, run_operator, looptime, caplog, code):
    seen: list[str] = []

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples', registry=registry)
    def fn(event, **_):
        seen.append(event['object']['metadata']['name'])

    kmock['watch', EXAMPLES] << once((
        {'type': 'ADDED', 'object': {'metadata': {'name': 'before', 'namespace': 'ns',
                                                  'resourceVersion': '101'}}},
        {'type': 'ERROR', 'object': {'kind': 'Status', 'code': code, 'reason': 'Temporary',
                                     'message': 'Temporarily unavailable.'}},
    ))

    with caplog.at_level(logging.DEBUG):
        async with run_operator() as task:
            await wait_for(lambda: len(kmock['watch', EXAMPLES]) >= 2 or task.done(), kmock=kmock)
            await asyncio.sleep(1)
            await kmock.post('/apis/kopf.dev/v1/namespaces/ns/kopfexamples',
                             json={'metadata': {'name': 'after', 'namespace': 'ns'}})
            await wait_for(lambda: 'after' in seen or task.done(), kmock=kmock)
            running = not task.done()

    assert running  # the operator did not stop
    assert seen == ['before', 'after']
    assert len(kmock['list', EXAMPLES]) == 1  # no new listing: the stream resumes where it was
    assert len(kmock['watch', EXAMPLES]) == 2
    assert kmock['watch', EXAMPLES][1].params['resourceVersion'] == '101'
    assert any('failed; reconnecting' in rec.getMessage() for rec in caplog.records)
    assert not [rec for rec in caplog.records if rec.levelno >= logging.ERROR]


@pytest.mark.parametrize('status', [429, 503])
@pytest.mark.parametrize('headers, details, expected', [
    pytest.param({'Retry-After': '30'}, {}, 30, id='seconds'),
    pytest.param(HTTP_DATE, {}, 30, id='http-date'),
    pytest.param({}, {'retryAfterSeconds': 30}, 30, id='status-details'),
])
async def test_retry_after_of_a_failed_watch_request_is_obeyed(
        kmock, registry, run_operator, looptime, status, headers, details, expected):
    times: list[float] = []
    if headers is HTTP_DATE:  # built now, not at collection, so that the 30 s are not used up
        headers = {'Retry-After': http_date(expected)}

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples', registry=registry)
    def fn(**_):
        pass

    kmock['watch', EXAMPLES] << once(Response(
        status=status, headers=headers,
        payload={'kind': 'Status', 'code': status, 'reason': 'Temporary', 'details': details},
    ), times=times)

    async with run_operator() as task:
        await wait_for(lambda: len(times) >= 2 or task.done(), kmock=kmock, timeout=1000)
        running = not task.done()

    assert running  # the operator did not stop
    assert expected - 1 <= times[1] - times[0] <= expected + 1


@pytest.mark.parametrize('code', [429, 503])
@pytest.mark.parametrize('requested, expected', [
    pytest.param(30, 30, id='below-the-cap'),
    pytest.param(99999, 300, id='capped-at-5-minutes'),
])
async def test_retry_after_in_an_error_event_is_obeyed_up_to_5_minutes(
        kmock, registry, run_operator, looptime, code, requested, expected):
    times: list[float] = []

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples', registry=registry)
    def fn(**_):
        pass

    kmock['watch', EXAMPLES] << once((
        {'type': 'ERROR', 'object': {'kind': 'Status', 'code': code, 'reason': 'Temporary',
                                     'details': {'retryAfterSeconds': requested}}},
    ), times=times)

    async with run_operator() as task:
        await wait_for(lambda: len(times) >= 2 or task.done(), kmock=kmock, timeout=1000)
        running = not task.done()

    assert running  # the operator did not stop
    assert expected - 1 <= times[1] - times[0] <= expected + 1


async def test_peering_outage_pauses_the_handling_and_recovers(
        kmock, mocker, settings, registry, run_operator, looptime, caplog):
    seen: list[str] = []
    mocker.patch('random.randint', return_value=50)  # the renewals are at 30s, 60s, ...
    settings.peering.lifetime = 60
    settings.networking.error_backoffs = []  # no retries inside the request: the keep-alive decides

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples', registry=registry)
    def fn(event, **_):
        seen.append(event['object']['metadata']['name'])

    await create_peering(kmock)
    await kmock.post('/apis/kopf.dev/v1/namespaces/ns/kopfexamples',
                     json={'metadata': {'name': 'before', 'namespace': 'ns'}})
    t0 = asyncio.get_running_loop().time()
    kmock['patch', PEERINGS] << outage(begin=t0 + 29, end=t0 + 40)

    with caplog.at_level(logging.DEBUG):
        async with run_operator(standalone=False) as task:
            await wait_for(lambda: 'before' in seen or task.done(), kmock=kmock)

            await sleep_until(t0 + 33)  # the renewal at 30s has failed and is being retried
            await kmock.post('/apis/kopf.dev/v1/namespaces/ns/kopfexamples',
                             json={'metadata': {'name': 'during', 'namespace': 'ns'}})
            await sleep_until(t0 + 38)
            seen_during_outage = list(seen)

            await sleep_until(t0 + 49)  # recovered at ~42s
            seen_after_outage = list(seen)
            running = not task.done()

    assert running  # the operator did not stop
    assert 'during' not in seen_during_outage  # the handling is paused
    assert 'during' in seen_after_outage  # and it resumes by itself
    assert any('Keep-alive' in rec.getMessage() and 'pausing and retrying' in rec.getMessage()
               for rec in caplog.records)


async def test_peering_outage_beyond_the_record_expiry_stays_paused_and_recovers_in_place(
        kmock, mocker, settings, registry, run_operator, looptime, caplog):
    seen: list[str] = []
    mocker.patch('random.randint', return_value=50)  # the renewals are at 30s, ...
    settings.peering.lifetime = 60
    settings.watching.reconnect_backoff = 5
    settings.networking.error_backoffs = []  # no retries inside the request: the keep-alive decides

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples', registry=registry)
    def fn(event, **_):
        seen.append(event['object']['metadata']['name'])

    await create_peering(kmock)
    await kmock.post('/apis/kopf.dev/v1/namespaces/ns/kopfexamples',
                     json={'metadata': {'name': 'before', 'namespace': 'ns'}})
    t0 = asyncio.get_running_loop().time()
    # Outage lasts from 29s until 80s -- well past the 60s expiry of the initial record.
    kmock['patch', PEERINGS] << outage(begin=t0 + 29, end=t0 + 80)

    with caplog.at_level(logging.DEBUG):
        async with run_operator(standalone=False) as task:
            await wait_for(lambda: 'before' in seen or task.done(), kmock=kmock)

            await sleep_until(t0 + 65)  # past the 60s expiry; still in outage
            await kmock.post('/apis/kopf.dev/v1/namespaces/ns/kopfexamples',
                             json={'metadata': {'name': 'during-long-outage', 'namespace': 'ns'}})
            await sleep_until(t0 + 75)
            seen_past_expiry = list(seen)
            running_during_outage = not task.done()

            await wait_for(
                lambda: 'during-long-outage' in seen or task.done(), kmock=kmock, timeout=60)
            running_after_recovery = not task.done()

    assert running_during_outage and running_after_recovery  # the operator never stops
    assert 'during-long-outage' not in seen_past_expiry  # stayed paused past record expiry
    assert 'during-long-outage' in seen  # resumed in place once keep-alive succeeded
    assert any('Keep-alive' in rec.getMessage() and 'pausing and retrying' in rec.getMessage()
               for rec in caplog.records)


async def test_failed_cleaning_of_dead_peers_does_not_stop_the_peering_observer(
        kmock, settings, registry, run_operator, looptime, caplog):
    seen: list[str] = []
    settings.peering.lifetime = 60
    settings.networking.error_backoffs = []

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples', registry=registry)
    def fn(event, **_):
        seen.append(event['object']['metadata']['name'])

    await kmock.post('/apis/kopf.dev/v1/clusterkopfpeerings', json={
        'metadata': {'name': 'default'},
        'status': {'ghost': {'priority': 0, 'lifetime': 60,
                             'lastseen': '2000-01-01T00:00:00+00:00'}},
    })
    t0 = asyncio.get_running_loop().time()
    kmock['patch', PEERINGS] << outage(
        only=lambda request: 'ghost' in (request.data or {}).get('status', {}))  # all the time

    with caplog.at_level(logging.DEBUG):
        async with run_operator(standalone=False) as task:
            await sleep_until(t0 + 5)
            running_after_failure = not task.done()

            # The observer must still react to the peering changes: a rival of a higher priority
            # appears, so the operator must pause the handling, though the cleaning keeps failing.
            now = datetime.datetime.now(datetime.UTC).isoformat()
            await kmock.patch('/apis/kopf.dev/v1/clusterkopfpeerings/default', json={
                'status': {'rival': {'priority': 100, 'lifetime': 60, 'lastseen': now}}})
            await sleep_until(t0 + 10)
            await kmock.post('/apis/kopf.dev/v1/namespaces/ns/kopfexamples',
                             json={'metadata': {'name': 'late', 'namespace': 'ns'}})
            await sleep_until(t0 + 15)
            running = not task.done()

    assert running_after_failure and running  # the observer did not stop the operator
    assert 'late' not in seen  # and the observer still pauses the handling for the rival
    assert any('Cleaning the dead peers' in rec.getMessage() and rec.levelno == logging.WARNING
               for rec in caplog.records)


async def test_pause_cancels_a_watch_request_waiting_for_the_response_headers(
        kmock, mocker, settings, registry, run_operator, looptime, caplog):
    seen: list[str] = []
    mocker.patch('random.randint', return_value=50)  # the renewals are at 30s, 60s, ...
    settings.peering.lifetime = 60
    settings.networking.error_backoffs = []

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples', registry=registry)
    def fn(event, **_):
        seen.append(event['object']['metadata']['name'])

    await create_peering(kmock)
    t0 = asyncio.get_running_loop().time()
    # The first watch request of the resource gets no response headers at all, e.g. a silent proxy.
    kmock['watch', EXAMPLES] << once((lambda: asyncio.sleep(3600),))
    kmock['patch', PEERINGS] << outage(begin=t0 + 29, end=t0 + 40)

    async with run_operator(standalone=False) as task:
        await sleep_until(t0 + 20)  # the watch request is waiting for the headers (limit: 70s)
        await kmock.post('/apis/kopf.dev/v1/namespaces/ns/kopfexamples',
                         json={'metadata': {'name': 'late', 'namespace': 'ns'}})
        await sleep_until(t0 + 29)
        seen_before_pause = list(seen)
        await sleep_until(t0 + 49)  # paused at 30s, resumed at ~42s; below the 70s inactivity limit
        running = not task.done()

    assert running  # the operator did not stop
    assert seen_before_pause == []  # the silent request has really blocked the handling
    assert 'late' in seen  # the pause has cancelled it, so the resumed watch gets the object
    assert len(kmock['watch', EXAMPLES]) == 2
    assert not [r for r in caplog.records if r.name == 'asyncio']  # no crashed callbacks


@pytest.mark.parametrize('where', ['in-stream', 'http-response'])
async def test_authorization_errors_of_a_watch_stop_the_operator(
        kmock, settings, registry, run_operator, looptime, where):
    settings.networking.error_backoffs = []  # no retries inside the request

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples', registry=registry)
    def fn(**_):
        pass

    status = {'kind': 'Status', 'code': 403, 'reason': 'Forbidden', 'message': 'Forbidden.'}
    kmock['watch', EXAMPLES] << once(
        ({'type': 'ERROR', 'object': status},) if where == 'in-stream' else
        Response(status=403, payload=status)
    )

    async with run_operator() as task:
        await wait_for(task.done, kmock=kmock, timeout=100)

    assert task.done()  # the restart with fresh credentials is the only way to recover
    expected = watching.WatchingError if where == 'in-stream' else errors.APIForbiddenError
    assert isinstance(task.exception(), expected)
    assert len(kmock['watch', EXAMPLES]) == 1


async def test_http_retry_after_longer_than_the_inactivity_timeout_is_cut_short(
        kmock, settings, registry, run_operator, looptime, caplog):
    """
    A known limit, pinned on purpose: the watch reconnects when the inactivity timer fires.

    The server asks to wait for more than a day. The request's own retries obey it without a cap,
    but the wait runs inside the request, which is inside the inactivity timer, so the timer fires
    first and the watch reconnects after ``settings.watching.inactivity_timeout`` seconds.
    The rest of the Retry-After is not waited. For in-stream error events, the reconnect loop
    caps it at 5 minutes instead
    (see ``test_retry_after_in_an_error_event_is_obeyed_up_to_5_minutes``).

    If the limit is lifted, this test must change together with the fix.
    """
    times: list[float] = []

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples', registry=registry)
    def fn(**_):
        pass

    kmock['watch', EXAMPLES] << once(Response(
        status=503, headers={'Retry-After': '99999'},
        payload={'kind': 'Status', 'code': 503, 'reason': 'Temporary', 'message': 'Outage.'},
    ), times=times)

    with caplog.at_level(logging.DEBUG):
        async with run_operator() as task:
            await wait_for(lambda: len(times) >= 2 or task.done(), kmock=kmock, timeout=1000)
            running = not task.done()

    assert running  # the operator did not stop
    assert times[1] - times[0] == pytest.approx(settings.watching.inactivity_timeout, abs=1)
    assert any('is inactive, reconnecting' in rec.getMessage() for rec in caplog.records)
