import asyncio
import logging

import kopf
from kopf.reactor.causation import CREATE, UPDATE, DELETE, NEW, GONE, FREE, NOOP
from kopf.reactor.handling import custom_object_handler
from kopf.structs.finalizers import FINALIZER
from kopf.structs.lastseen import LAST_SEEN_ANNOTATION


async def test_new(registry, handlers, resource, cause_mock,
                   caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = NEW

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert k8s_mocked.asyncio_sleep.call_count == 0
    assert k8s_mocked.post_event.call_count == 0
    assert k8s_mocked.patch_obj.call_count == 1

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'metadata' in patch
    assert 'finalizers' in patch['metadata']
    assert FINALIZER in patch['metadata']['finalizers']

    assert_logs([
        "First appearance",
        "Adding the finalizer",
        "Patching with",
    ])


async def test_create(registry, handlers, resource, cause_mock,
                      caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = CREATE

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
    )

    assert handlers.create_mock.call_count == 1
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert k8s_mocked.asyncio_sleep.call_count == 0
    assert k8s_mocked.post_event.call_count >= 1
    assert k8s_mocked.patch_obj.call_count == 1

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'metadata' in patch
    assert 'annotations' in patch['metadata']
    assert LAST_SEEN_ANNOTATION in patch['metadata']['annotations']
    assert 'status' in patch
    assert 'kopf' in patch['status']
    assert 'progress' in patch['status']['kopf']
    assert patch['status']['kopf']['progress'] is None  # 1 out of 1 handlers done

    assert_logs([
        "Creation event:",
        "Invoking handler 'create_fn'",
        "Handler 'create_fn' succeeded",
        "All handlers succeeded",
        "Patching with",
    ])


async def test_update(registry, handlers, resource, cause_mock,
                      caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = UPDATE

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
    )

    assert not handlers.create_mock.called
    assert handlers.update_mock.call_count == 1
    assert not handlers.delete_mock.called

    assert k8s_mocked.asyncio_sleep.call_count == 0
    assert k8s_mocked.post_event.call_count >= 1
    assert k8s_mocked.patch_obj.call_count == 1

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'metadata' in patch
    assert 'annotations' in patch['metadata']
    assert LAST_SEEN_ANNOTATION in patch['metadata']['annotations']
    assert 'status' in patch
    assert 'kopf' in patch['status']
    assert 'progress' in patch['status']['kopf']
    assert patch['status']['kopf']['progress'] is None  # 1 out of 1 handlers done

    assert_logs([
        "Update event:",
        "Invoking handler 'update_fn'",
        "Handler 'update_fn' succeeded",
        "All handlers succeeded",
        "Patching with",
    ])


async def test_delete(registry, handlers, resource, cause_mock,
                      caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = DELETE

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert handlers.delete_mock.call_count == 1

    assert k8s_mocked.asyncio_sleep.call_count == 0
    assert k8s_mocked.post_event.call_count >= 1
    assert k8s_mocked.patch_obj.call_count == 1

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert 'status' in patch
    assert 'kopf' in patch['status']
    assert 'progress' in patch['status']['kopf']
    assert patch['status']['kopf']['progress'] is None  # 1 out of 1 handlers done

    assert_logs([
        "Deletion event",
        "Invoking handler 'delete_fn'",
        "Handler 'delete_fn' succeeded",
        "All handlers succeeded",
        "Removing the finalizer",
        "Patching with",
    ])


#
# Informational causes: just log, and do nothing else.
#

async def test_gone(registry, handlers, resource, cause_mock,
                    caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = GONE

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert not k8s_mocked.asyncio_sleep.called
    assert not k8s_mocked.post_event.called
    assert not k8s_mocked.patch_obj.called

    assert_logs([
        "Deleted, really deleted",
    ])


async def test_free(registry, handlers, resource, cause_mock,
                    caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = FREE

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert not k8s_mocked.asyncio_sleep.called
    assert not k8s_mocked.post_event.called
    assert not k8s_mocked.patch_obj.called

    assert_logs([
        "Deletion event, but we are done with it",
    ])


async def test_noop(registry, handlers, resource, cause_mock,
                    caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = NOOP

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
    )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called

    assert not k8s_mocked.asyncio_sleep.called
    assert not k8s_mocked.post_event.called
    assert not k8s_mocked.patch_obj.called

    assert_logs([
        "Something has changed, but we are not interested",
    ])
