import asyncio

import pytest
from asynctest import call

from kopf import event, info, warn, exception
from kopf.engines.posting import poster, K8sEvent, event_queue_var

OBJ1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}}
REF1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}
OBJ2 = {'apiVersion': 'group2/version2', 'kind': 'Kind2',
        'metadata': {'uid': 'uid2', 'name': 'name2', 'namespace': 'ns2'}}
REF2 = {'apiVersion': 'group2/version2', 'kind': 'Kind2',
        'uid': 'uid2', 'name': 'name2', 'namespace': 'ns2'}


async def test_poster_polls_and_posts(mocker):
    event1 = K8sEvent(type='type1', reason='reason1', message='message1', ref=REF1)
    event2 = K8sEvent(type='type2', reason='reason2', message='message2', ref=REF2)
    event_queue = asyncio.Queue()
    event_queue.put_nowait(event1)
    event_queue.put_nowait(event2)

    # A way to cancel `while True` cycle when we need it (ASAP).
    def _cancel(*args, **kwargs):
        if post_event.call_count >= 2:
            raise asyncio.CancelledError()
    post_event = mocker.patch('kopf.clients.events.post_event', side_effect=_cancel)

    # A way to cancel `whole True` cycle by timing, event if routines are not called.
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(
            poster(event_queue=event_queue), timeout=0.5)

    assert post_event.call_count == 2
    assert post_event.await_count == 2
    assert post_event.called_with(
        call(ref=REF1, type='type1', reason='reason1', message='message1'),
        call(ref=REF2, type='type2', reason='reason2', message='message2'),
    )


def test_queueing_fails_with_no_queue(mocker):
    # Prerequisite: the context-var should not be set by anything in advance.
    sentinel = object()
    assert event_queue_var.get(sentinel) is sentinel

    with pytest.raises(LookupError):
        event(OBJ1, type='type1', reason='reason1', message='message1')


def test_via_event_function(mocker):
    post_event = mocker.patch('kopf.clients.events.post_event')

    event_queue = asyncio.Queue()
    event_queue_var.set(event_queue)
    event(OBJ1, type='type1', reason='reason1', message='message1')

    assert not post_event.called
    assert event_queue.qsize() == 1
    event1 = event_queue.get_nowait()

    assert isinstance(event1, K8sEvent)
    assert event1.ref == REF1
    assert event1.type == 'type1'
    assert event1.reason == 'reason1'
    assert event1.message == 'message1'


@pytest.mark.parametrize('event_fn, event_type', [
    pytest.param(info, "Normal", id='info'),
    pytest.param(warn, "Warning", id='warn'),
    pytest.param(exception, "Error", id='exception'),
])
def test_via_shortcut(mocker, event_fn, event_type):
    post_event = mocker.patch('kopf.clients.events.post_event')

    event_queue = asyncio.Queue()
    event_queue_var.set(event_queue)
    event_fn(OBJ1, reason='reason1', message='message1')

    assert not post_event.called
    assert event_queue.qsize() == 1
    event1 = event_queue.get_nowait()

    assert isinstance(event1, K8sEvent)
    assert event1.ref == REF1
    assert event1.type == event_type
    assert event1.reason == 'reason1'
    assert event1.message == 'message1'
