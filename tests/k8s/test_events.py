import pytest

from kopf._cogs.clients.events import post_event
from kopf._cogs.structs.bodies import build_object_reference
from kopf._cogs.structs.references import Resource

EVENTS = Resource('', 'v1', 'events', namespaced=True)


async def test_posting(kmock, settings, logger):
    kmock['post v1/events', kmock.namespace('ns')] << {}

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(
        ref=ref,
        type='type',
        reason='reason',
        message='message',
        resource=EVENTS,
        settings=settings,
        logger=logger,
    )

    assert len(kmock) == 1
    assert kmock[-1].method == 'POST'
    data = kmock[-1].data
    assert data['type'] == 'type'
    assert data['reason'] == 'reason'
    assert data['message'] == 'message'
    assert data['source']['component'] == 'kopf'
    assert data['involvedObject']['apiVersion'] == 'group/version'
    assert data['involvedObject']['kind'] == 'kind'
    assert data['involvedObject']['namespace'] == 'ns'
    assert data['involvedObject']['name'] == 'name'
    assert data['involvedObject']['uid'] == 'uid'


async def test_no_events_for_events(kmock, settings, logger):
    post = kmock['post v1/events', kmock.namespace('ns')] << {}

    obj = {'apiVersion': 'v1',
           'kind': 'Event',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(
        ref=ref,
        type='type',
        reason='reason',
        message='message',
        resource=EVENTS,
        settings=settings,
        logger=logger,
    )

    assert len(post) == 0


async def test_api_errors_logged_but_suppressed(kmock, settings, logger, assert_logs):
    post = kmock['post v1/events', kmock.namespace('ns')] << 555

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(
        ref=ref,
        type='type',
        reason='reason',
        message='message',
        resource=EVENTS,
        settings=settings,
        logger=logger,
    )

    assert len(post) == 1
    assert_logs(["Failed to post an event."])


async def test_regular_errors_escalate(enforced_session, mocker, settings, logger):
    error = Exception('boo!')
    enforced_session.request = mocker.Mock(side_effect=error)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)

    with pytest.raises(Exception) as excinfo:
        await post_event(
            ref=ref,
            type='type',
            reason='reason',
            message='message',
            resource=EVENTS,
            settings=settings,
            logger=logger,
        )

    assert excinfo.value is error


async def test_message_is_cut_to_max_length(kmock, settings, logger):
    post = kmock['post v1/events', kmock.namespace('ns')] << {}

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    message = 'start' + ('x' * 2048) + 'end'
    await post_event(
        ref=ref,
        type='type',
        reason='reason',
        message=message,
        resource=EVENTS,
        settings=settings,
        logger=logger,
    )

    data = post[0].data
    assert len(data['message']) <= 1024  # max supported API message length
    assert '...' in data['message']
    assert data['message'].startswith('start')
    assert data['message'].endswith('end')


# 401 causes LoginError from the vault, and this is out of scope of API testing.
@pytest.mark.parametrize('status', [555, 500, 404, 403])
async def test_headers_are_not_leaked(kmock, settings, logger, assert_logs, status):
    kmock['post v1/events', kmock.namespace('ns')] << status

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(
        ref=ref,
        type='type',
        reason='reason',
        message='message',
        resource=EVENTS,
        settings=settings,
        logger=logger,
    )

    assert_logs([
        "Failed to post an event.",
    ], prohibited=[
        "ClientResponseError",
        "RequestInfo",
        "headers=",
    ])
