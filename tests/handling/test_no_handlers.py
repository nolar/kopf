import asyncio
import logging

import pytest

import kopf
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.processing import process_resource_event
from kopf.structs.containers import ResourceMemories
from kopf.structs.ephemera import Memo
from kopf.structs.handlers import HANDLER_REASONS, ResourceChangingHandler

LAST_SEEN_ANNOTATION = 'kopf.zalando.org/last-handled-configuration'


@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_skipped_with_no_handlers(
        registry, settings, selector, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)

    event_type = None
    event_body = {'metadata': {'finalizers': []}}
    cause_mock.reason = cause_type

    assert not registry._resource_changing.has_handlers(resource=resource)  # prerequisite
    registry._resource_changing.append(ResourceChangingHandler(
        reason='a-non-existent-cause-type',
        fn=lambda **_: None, id='id', param=None,
        errors=None, timeout=None, retries=None, backoff=None,
        selector=selector, annotations=None, labels=None, when=None,
        field=None, value=None, old=None, new=None, field_needs_change=None,
        deleted=None, initial=None, requires_finalizer=None,
    ))

    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': event_body},
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
        "(Creation|Updating|Resuming|Deletion) is in progress:",
        "Patching with:",
    ], prohibited=[
        "(Creation|Updating|Resuming|Deletion) is processed:",
    ])


@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
@pytest.mark.parametrize('initial', [True, False, None])
@pytest.mark.parametrize('deleted', [True, False, None])
@pytest.mark.parametrize('annotations, labels, when', [
    (None, {'some-label': '...'}, None),
    ({'some-annotation': '...'}, None, None),
    (None, None, lambda **_: False),
])
async def test_stealth_mode_with_mismatching_handlers(
        registry, settings, selector, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked, annotations, labels, when, deleted, initial):
    caplog.set_level(logging.DEBUG)

    event_type = None
    event_body = {'metadata': {'finalizers': []}}
    cause_mock.reason = cause_type

    assert not registry._resource_changing.has_handlers(resource=resource)  # prerequisite
    registry._resource_changing.append(ResourceChangingHandler(
        reason=None,
        fn=lambda **_: None, id='id', param=None,
        errors=None, timeout=None, retries=None, backoff=None,
        selector=selector, annotations=annotations, labels=labels, when=when,
        field=None, value=None, old=None, new=None, field_needs_change=None,
        deleted=deleted, initial=initial, requires_finalizer=None,
    ))

    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': event_body},
        event_queue=asyncio.Queue(),
    )

    assert not k8s_mocked.sleep_or_wait.called
    assert not k8s_mocked.patch_obj.called
    assert not caplog.messages  # total stealth mode!
