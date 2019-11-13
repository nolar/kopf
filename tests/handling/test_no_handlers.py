import asyncio
import logging

import pytest

import kopf
from kopf.reactor.causation import HANDLER_REASONS
from kopf.reactor.handling import resource_handler
from kopf.structs.containers import ResourceMemories
from kopf.structs.lastseen import LAST_SEEN_ANNOTATION


@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_skipped_with_no_handlers(
        registry, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = cause_type

    assert not registry.has_resource_changing_handlers(resource=resource)  # prerequisite
    registry.register_resource_changing_handler(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        reason='a-non-existent-cause-type',
        fn=lambda **_: None,
    )
    assert registry.has_resource_changing_handlers(resource=resource)  # prerequisite

    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': None, 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )

    assert not k8s_mocked.asyncio_sleep.called
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
