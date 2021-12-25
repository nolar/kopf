import asyncio
import logging

import pytest

from kopf import event, exception, info, warn
from kopf._cogs.structs.references import Backbone, Resource
from kopf._core.engines.posting import K8sEvent, event_queue_loop_var, event_queue_var, poster

OBJ1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}}
REF1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}
OBJ2 = {'apiVersion': 'group2/version2', 'kind': 'Kind2',
        'metadata': {'uid': 'uid2', 'name': 'name2', 'namespace': 'ns2'}}
REF2 = {'apiVersion': 'group2/version2', 'kind': 'Kind2',
        'uid': 'uid2', 'name': 'name2', 'namespace': 'ns2'}

EVENTS = Resource('', 'v1', 'events', namespaced=True)


@pytest.fixture(autouse=True)
def _settings_via_contextvar(settings_via_contextvar):
    pass


async def test_poster_polls_and_posts(mocker, settings):

    event1 = K8sEvent(type='type1', reason='reason1', message='message1', ref=REF1)
    event2 = K8sEvent(type='type2', reason='reason2', message='message2', ref=REF2)
    event_queue = asyncio.Queue()
    event_queue.put_nowait(event1)
    event_queue.put_nowait(event2)

    # A way to cancel `while True` cycle when we need it (ASAP).
    def _cancel(*args, **kwargs):
        if post.call_count >= 2:
            raise asyncio.CancelledError()
    post = mocker.patch('kopf._cogs.clients.api.post', side_effect=_cancel)

    backbone = Backbone()
    await backbone.fill(resources=[EVENTS])

    # A way to cancel a `while True` cycle by timing, even if the routines are not called.
    with pytest.raises(asyncio.CancelledError):
        await poster(event_queue=event_queue, backbone=backbone, settings=settings)

    assert post.call_count == 2
    assert post.call_args_list[0][1]['url'] == '/api/v1/namespaces/ns1/events'
    assert post.call_args_list[0][1]['payload']['type'] == 'type1'
    assert post.call_args_list[0][1]['payload']['reason'] == 'reason1'
    assert post.call_args_list[0][1]['payload']['message'] == 'message1'
    assert post.call_args_list[0][1]['payload']['involvedObject'] == REF1
    assert post.call_args_list[1][1]['url'] == '/api/v1/namespaces/ns2/events'
    assert post.call_args_list[1][1]['payload']['type'] == 'type2'
    assert post.call_args_list[1][1]['payload']['reason'] == 'reason2'
    assert post.call_args_list[1][1]['payload']['message'] == 'message2'
    assert post.call_args_list[1][1]['payload']['involvedObject'] == REF2


def test_queueing_fails_with_no_queue(event_queue_loop):
    # Prerequisite: the context-var should not be set by anything in advance.
    sentinel = object()
    assert event_queue_var.get(sentinel) is sentinel

    with pytest.raises(LookupError):
        event(OBJ1, type='type1', reason='reason1', message='message1')


def test_queueing_fails_with_no_loop(event_queue):
    # Prerequisite: the context-var should not be set by anything in advance.
    sentinel = object()
    assert event_queue_loop_var.get(sentinel) is sentinel

    with pytest.raises(LookupError):
        event(OBJ1, type='type1', reason='reason1', message='message1')


async def test_via_event_function(mocker, event_queue, event_queue_loop):
    post = mocker.patch('kopf._cogs.clients.api.post')

    event(OBJ1, type='type1', reason='reason1', message='message1')

    assert not post.called
    assert event_queue.qsize() == 1
    event1 = event_queue.get_nowait()

    assert isinstance(event1, K8sEvent)
    assert event1.ref == REF1
    assert event1.type == 'type1'
    assert event1.reason == 'reason1'
    assert event1.message == 'message1'


@pytest.mark.parametrize('event_fn, event_type, min_levelno', [
    pytest.param(info, "Normal", logging.INFO, id='info'),
    pytest.param(warn, "Warning", logging.WARNING, id='warn'),
    pytest.param(exception, "Error", logging.ERROR, id='exception'),
])
async def test_via_shortcut(settings, mocker, event_fn, event_type, min_levelno,
                            event_queue, event_queue_loop):
    post = mocker.patch('kopf._cogs.clients.api.post')

    settings.posting.level = min_levelno
    event_fn(OBJ1, reason='reason1', message='message1')  # posted
    settings.posting.level = min_levelno + 1
    event_fn(OBJ1, reason='reason2', message='message2')  # not posted

    assert not post.called
    assert event_queue.qsize() == 1
    event1 = event_queue.get_nowait()

    assert isinstance(event1, K8sEvent)
    assert event1.ref == REF1
    assert event1.type == event_type
    assert event1.reason == 'reason1'
    assert event1.message == 'message1'
