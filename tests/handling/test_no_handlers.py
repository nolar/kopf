import asyncio
import logging

import pytest

import kopf
from kopf.reactor.causation import HANDLER_CAUSES
from kopf.reactor.handling import custom_object_handler
from kopf.structs.lastseen import LAST_SEEN_ANNOTATION


@pytest.mark.parametrize('cause_type', HANDLER_CAUSES)
async def test_skipped_with_no_handlers(
        registry, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = cause_type

    assert not registry.has_cause_handlers(resource=resource)  # prerequisite
    registry.register_cause_handler(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        event='a-non-existent-cause-type',
        fn=lambda **_: None,
    )
    assert registry.has_cause_handlers(resource=resource)  # prerequisite

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )

    assert not k8s_mocked.asyncio_sleep.called
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
