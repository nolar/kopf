import asyncio
import logging

import pytest

import kopf
from kopf.reactor.processing import process_resource_event
from kopf.storage.diffbase import LAST_SEEN_ANNOTATION
from kopf.structs.containers import ResourceMemories
from kopf.structs.handlers import ResourceChangingHandler, HANDLER_REASONS


@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_skipped_with_no_handlers(
        registry, settings, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)

    event_type = None
    event_body = {'metadata': {'finalizers': []}}
    cause_mock.reason = cause_type

    assert not registry.resource_changing_handlers[resource]  # prerequisite
    registry.resource_changing_handlers[resource].append(ResourceChangingHandler(
        reason='a-non-existent-cause-type',
        fn=lambda **_: None, id='id',
        errors=None, timeout=None, retries=None, backoff=None, cooldown=None,
        annotations=None, labels=None, when=None, field=None,
        deleted=None, initial=None, requires_finalizer=None,
    ))

    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        memories=ResourceMemories(),
        raw_event={'type': event_type, 'object': event_body},
        replenished=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )

    assert not k8s_mocked.sleep_or_wait.called
    assert k8s_mocked.patch_obj.called

    # The patch must contain ONLY the last-seen update, and nothing else.
    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert set(patch.keys()) == {'metadata'}
    assert set(patch['metadata'].keys()) == {'annotations'}
    assert set(patch['metadata']['annotations'].keys()) == {LAST_SEEN_ANNOTATION}

    assert_logs([
        ".* event:",
        "Patching with:",
    ], prohibited=[
        "All handlers succeeded",
    ])
