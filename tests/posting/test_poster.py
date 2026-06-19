import asyncio
import logging

import pytest

from kopf import event, exception, info, warn
from kopf._cogs.structs.references import Backbone, Resource
from kopf._core.engines.posting import WORKER_IDLE_TIMEOUT, K8sEvent, \
                                       event_queue_loop_var, event_queue_var, poster

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


async def test_poster_posts_all_events(mocker, settings):
    event1 = K8sEvent(type='type1', reason='reason1', message='message1', ref=REF1)
    event2 = K8sEvent(type='type2', reason='reason2', message='message2', ref=REF2)
    event_queue = asyncio.Queue()
    event_queue.put_nowait(event1)
    event_queue.put_nowait(event2)

    post = mocker.patch('kopf._cogs.clients.api.post')
    backbone = Backbone()
    await backbone.fill(resources=[EVENTS])

    poster_task = asyncio.create_task(
        poster(event_queue=event_queue, backbone=backbone, settings=settings))

    # Virtual time: let the router + per-object workers drain the queue and post.
    await asyncio.sleep(WORKER_IDLE_TIMEOUT + 1)
    poster_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await poster_task

    posted_urls = {c.kwargs['url'] for c in post.call_args_list}
    assert post.call_count == 2
    assert '/api/v1/namespaces/ns1/events' in posted_urls
    assert '/api/v1/namespaces/ns2/events' in posted_urls


async def test_same_object_events_are_ordered(mocker, settings):
    event1 = K8sEvent(type='type1', reason='reason1', message='message1', ref=REF1)
    event2 = K8sEvent(type='type2', reason='reason2', message='message2', ref=REF1)
    event_queue = asyncio.Queue()
    event_queue.put_nowait(event1)
    event_queue.put_nowait(event2)

    post = mocker.patch('kopf._cogs.clients.api.post')
    backbone = Backbone()
    await backbone.fill(resources=[EVENTS])

    poster_task = asyncio.create_task(
        poster(event_queue=event_queue, backbone=backbone, settings=settings))
    await asyncio.sleep(WORKER_IDLE_TIMEOUT + 1)
    poster_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await poster_task

    messages = [c.kwargs['payload']['message'] for c in post.call_args_list]
    assert messages == ['message1', 'message2']


async def test_other_objects_not_blocked_by_a_retrying_object(mocker, settings, looptime):
    # Object 1's post hangs for a long time (simulating a retry backoff); object 2's
    # post is immediate. A single sequential poster would never reach object 2 until
    # object 1 finished, so object 2 being posted within 1s proves the workers run
    # independently per object.
    async def fake_post_event(*, ref, **kwargs):
        if ref.get('namespace') == 'ns1':
            await asyncio.sleep(100)

    post = mocker.patch('kopf._cogs.clients.events.post_event', side_effect=fake_post_event)

    event1 = K8sEvent(type='t', reason='r', message='m1', ref=REF1)
    event2 = K8sEvent(type='t', reason='r', message='m2', ref=REF2)
    event_queue = asyncio.Queue()
    event_queue.put_nowait(event1)
    event_queue.put_nowait(event2)

    backbone = Backbone()
    await backbone.fill(resources=[EVENTS])
    poster_task = asyncio.create_task(
        poster(event_queue=event_queue, backbone=backbone, settings=settings))

    # Advance only 1 virtual second: object 1's post is still mid-backoff (100s).
    await asyncio.sleep(1)
    posted_ns = {c.kwargs['ref'].get('namespace') for c in post.call_args_list}
    assert 'ns2' in posted_ns          # object 2 was posted...
    assert looptime < 100              # ...without waiting for object 1's backoff

    poster_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await poster_task


async def test_idle_worker_terminates(mocker, settings):
    event1 = K8sEvent(type='t', reason='r', message='m1', ref=REF1)
    event_queue = asyncio.Queue()
    event_queue.put_nowait(event1)

    post = mocker.patch('kopf._cogs.clients.api.post')
    backbone = Backbone()
    await backbone.fill(resources=[EVENTS])
    poster_task = asyncio.create_task(
        poster(event_queue=event_queue, backbone=backbone, settings=settings))

    # After the idle timeout, the per-object worker should have exited (queue drained).
    await asyncio.sleep(WORKER_IDLE_TIMEOUT + 1)
    # A second event for the same object must still be posted (worker re-spawned).
    event_queue.put_nowait(K8sEvent(type='t', reason='r', message='m2', ref=REF1))
    await asyncio.sleep(WORKER_IDLE_TIMEOUT + 1)
    poster_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await poster_task

    assert post.call_count == 2


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


async def test_event_function_uses_default_backoffs(settings, event_queue, event_queue_loop):
    settings.posting.default_backoffs = [3, 4]
    event(OBJ1, type='type1', reason='reason1', message='message1')
    e = event_queue.get_nowait()
    assert e.backoffs == [3, 4]


async def test_event_function_honors_explicit_backoffs(settings, event_queue, event_queue_loop):
    event(OBJ1, type='type1', reason='reason1', message='message1', backoffs=[9])
    e = event_queue.get_nowait()
    assert e.backoffs == [9]
