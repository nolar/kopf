import asyncio
import logging

import pytest

import kopf
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.processing import process_resource_event
from kopf.structs.containers import ResourceMemories
from kopf.structs.ephemera import Memo
from kopf.structs.handlers import Reason

LAST_SEEN_ANNOTATION = 'kopf.zalando.org/last-handled-configuration'

EVENT_TYPES = [None, 'ADDED', 'MODIFIED', 'DELETED']
EVENT_TYPES_WHEN_EXISTS = [None, 'ADDED', 'MODIFIED']


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_create(registry, settings, handlers, resource, cause_mock, event_type,
                      caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.CREATE

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=event_queue,
    )

    assert handlers.create_mock.call_count == 1
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert not event_queue.empty()

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'metadata' in patch
    assert 'annotations' in patch['metadata']
    assert LAST_SEEN_ANNOTATION in patch['metadata']['annotations']

    assert_logs([
        "Creation is in progress:",
        "Handler 'create_fn' is invoked",
        "Handler 'create_fn' succeeded",
        "Creation is processed:",
        "Patching with",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_update(registry, settings, handlers, resource, cause_mock, event_type,
                      caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.UPDATE

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert handlers.update_mock.call_count == 1
    assert not handlers.delete_mock.called

    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert not event_queue.empty()

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'metadata' in patch
    assert 'annotations' in patch['metadata']
    assert LAST_SEEN_ANNOTATION in patch['metadata']['annotations']

    assert_logs([
        "Updating is in progress:",
        "Handler 'update_fn' is invoked",
        "Handler 'update_fn' succeeded",
        "Updating is processed:",
        "Patching with",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_delete(registry, settings, handlers, resource, cause_mock, event_type,
                      caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.DELETE
    finalizer = settings.persistence.finalizer
    event_body = {'metadata': {'deletionTimestamp': '...', 'finalizers': [finalizer]}}

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': event_body},
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert handlers.delete_mock.call_count == 1

    assert k8s_mocked.sleep_or_wait.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1
    assert not event_queue.empty()

    assert_logs([
        "Deletion is in progress:",
        "Handler 'delete_fn' is invoked",
        "Handler 'delete_fn' succeeded",
        "Deletion is processed:",
        "Removing the finalizer",
        "Patching with",
    ])


#
# Informational causes: just log, and do nothing else.
#

@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_gone(registry, settings, handlers, resource, cause_mock, event_type,
                    caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.GONE

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert not k8s_mocked.patch_obj.called
    assert event_queue.empty()

    assert_logs([
        "Deleted, really deleted",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_free(registry, settings, handlers, resource, cause_mock, event_type,
                    caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.FREE

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert not k8s_mocked.sleep_or_wait.called
    assert not k8s_mocked.patch_obj.called
    assert event_queue.empty()

    assert_logs([
        "Deletion, but we are done with it",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES)
async def test_noop(registry, settings, handlers, resource, cause_mock, event_type,
                    caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.NOOP

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=event_queue,
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert not k8s_mocked.sleep_or_wait.called
    assert not k8s_mocked.patch_obj.called
    assert event_queue.empty()

    assert_logs([
        "Something has changed, but we are not interested",
    ])
