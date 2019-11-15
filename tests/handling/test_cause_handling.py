import asyncio
import logging

import pytest

import kopf
from kopf.reactor.causation import Reason
from kopf.reactor.handling import resource_handler
from kopf.structs.containers import ResourceMemories
from kopf.structs.finalizers import FINALIZER
from kopf.structs.lastseen import LAST_SEEN_ANNOTATION

EVENT_TYPES = [None, 'ADDED', 'MODIFIED', 'DELETED']


@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_acquire(registry, handlers, resource, cause_mock, event_type,
                   caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)

    cause_mock.reason = Reason.ACQUIRE

    event_queue = asyncio.Queue()
    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': event_type, 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert k8s_mocked.asyncio_sleep.call_count == 0
    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert event_queue.empty()

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'metadata' in patch
    assert 'finalizers' in patch['metadata']
    assert FINALIZER in patch['metadata']['finalizers']

    assert_logs([
        "Adding the finalizer",
        "Patching with",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_create(registry, handlers, resource, cause_mock, event_type,
                      caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.CREATE

    event_queue = asyncio.Queue()
    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': event_type, 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=event_queue,
    )

    assert handlers.create_mock.call_count == 1
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert k8s_mocked.asyncio_sleep.call_count == 0
    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert not event_queue.empty()

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'metadata' in patch
    assert 'annotations' in patch['metadata']
    assert LAST_SEEN_ANNOTATION in patch['metadata']['annotations']
    assert 'status' not in patch  # because only 1 handler, nothing to purge

    assert_logs([
        "Creation event:",
        "Invoking handler 'create_fn'",
        "Handler 'create_fn' succeeded",
        "All handlers succeeded",
        "Patching with",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_update(registry, handlers, resource, cause_mock, event_type,
                      caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.UPDATE

    event_queue = asyncio.Queue()
    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': event_type, 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert handlers.update_mock.call_count == 1
    assert not handlers.delete_mock.called

    assert k8s_mocked.asyncio_sleep.call_count == 0
    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert not event_queue.empty()

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'metadata' in patch
    assert 'annotations' in patch['metadata']
    assert LAST_SEEN_ANNOTATION in patch['metadata']['annotations']
    assert 'status' not in patch  # because only 1 handler, nothing to purge

    assert_logs([
        "Update event:",
        "Invoking handler 'update_fn'",
        "Handler 'update_fn' succeeded",
        "All handlers succeeded",
        "Patching with",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_delete(registry, handlers, resource, cause_mock, event_type,
                      caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.DELETE

    event_queue = asyncio.Queue()
    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': event_type, 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert handlers.delete_mock.call_count == 1

    assert k8s_mocked.asyncio_sleep.call_count == 0
    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert not event_queue.empty()

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'status' not in patch  # because only 1 handler, nothing to purge

    assert_logs([
        "Deletion event",
        "Invoking handler 'delete_fn'",
        "Handler 'delete_fn' succeeded",
        "All handlers succeeded",
        "Removing the finalizer",
        "Patching with",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_release(registry, resource, handlers, cause_mock, event_type,
                       caplog, k8s_mocked, assert_logs):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.RELEASE
    cause_mock.body.setdefault('metadata', {})['finalizers'] = [FINALIZER]

    # register handlers (no deletion handlers)
    registry.register_resource_changing_handler(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        reason=Reason.RESUME,
        fn=lambda **_: None,
        requires_finalizer=False,
    )

    event_queue = asyncio.Queue()
    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': event_type, 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert k8s_mocked.asyncio_sleep.call_count == 0
    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert event_queue.empty()

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'metadata' in patch
    assert 'finalizers' in patch['metadata']
    assert [] == patch['metadata']['finalizers']

    assert_logs([
        "Removing the finalizer",
        "Patching with",
    ])


#
# Informational causes: just log, and do nothing else.
#

@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_gone(registry, handlers, resource, cause_mock, event_type,
                    caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.GONE

    event_queue = asyncio.Queue()
    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': event_type, 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert not k8s_mocked.asyncio_sleep.called
    assert not k8s_mocked.sleep_or_wait.called
    assert not k8s_mocked.patch_obj.called
    assert event_queue.empty()

    assert_logs([
        "Deleted, really deleted",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_free(registry, handlers, resource, cause_mock, event_type,
                    caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.FREE

    event_queue = asyncio.Queue()
    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': event_type, 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert not k8s_mocked.asyncio_sleep.called
    assert not k8s_mocked.sleep_or_wait.called
    assert not k8s_mocked.patch_obj.called
    assert event_queue.empty()

    assert_logs([
        "Deletion event, but we are done with it",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_noop(registry, handlers, resource, cause_mock, event_type,
                    caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.NOOP

    event_queue = asyncio.Queue()
    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': event_type, 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert not k8s_mocked.asyncio_sleep.called
    assert not k8s_mocked.sleep_or_wait.called
    assert not k8s_mocked.patch_obj.called
    assert event_queue.empty()

    assert_logs([
        "Something has changed, but we are not interested",
    ])
