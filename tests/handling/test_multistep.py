import asyncio
import json

import pytest

import kopf
from kopf._cogs.structs.ephemera import Memo
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.intents.causes import HANDLER_REASONS, Reason
from kopf._core.reactor.inventory import ResourceMemories
from kopf._core.reactor.processing import process_resource_event


@pytest.mark.parametrize('deletion_ts', [
    pytest.param({}, id='no-deletion-ts'),
    pytest.param({'deletionTimestamp': None}, id='empty-deletion-ts'),
    pytest.param({'deletionTimestamp': 'some'}, id='real-deletion-ts'),
])
@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_1st_step_stores_progress_by_patching(
        registry, settings, handlers, extrahandlers,
        resource, cause_mock, cause_type, k8s_mocked, looptime, deletion_ts):
    name1 = f'{cause_type}_fn'
    name2 = f'{cause_type}_fn2'

    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    event_body = {
        'metadata': {'finalizers': [settings.persistence.finalizer]},
    }
    event_body['metadata'] |= deletion_ts
    cause_mock.reason = cause_type

    await process_resource_event(
        lifecycle=kopf.lifecycles.asap,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': event_body},
        event_queue=asyncio.Queue(),
    )

    assert handlers.create_mock.call_count == (1 if cause_type == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_type == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_type == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_type == Reason.RESUME else 0)

    assert looptime == 0
    assert k8s_mocked.patch.called

    patch = k8s_mocked.patch.call_args_list[0].kwargs['payload']
    assert patch['metadata']['annotations']  # not empty at least
    progress1 = json.loads(patch['metadata']['annotations'][f"kopf.zalando.org/{name1}"])
    progress2 = json.loads(patch['metadata']['annotations'][f"kopf.zalando.org/{name2}"])

    assert progress1['retries'] == 1
    assert progress1['success'] is True

    assert progress2['retries'] == 0
    assert progress2['success'] is False

    assert progress1['started']
    assert progress2['started']

    # Premature removal of finalizers can prevent the 2nd step for deletion handlers.
    # So, the finalizers must never be removed on the 1st step.
    assert 'finalizers' not in patch['metadata']


@pytest.mark.parametrize('deletion_ts', [
    pytest.param({}, id='no-deletion-ts'),
    pytest.param({'deletionTimestamp': None}, id='empty-deletion-ts'),
    pytest.param({'deletionTimestamp': 'some'}, id='real-deletion-ts'),
])
@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_2nd_step_finishes_the_handlers(caplog,
        registry, settings, handlers, extrahandlers,
        resource, cause_mock, cause_type, k8s_mocked, looptime, deletion_ts):
    name1 = f'{cause_type}_fn'
    name2 = f'{cause_type}_fn2'

    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    event_body = {
        'metadata': {
            'finalizers': [settings.persistence.finalizer],
            'annotations': {
                f'kopf.zalando.org/{name1}': json.dumps({
                    'started': '1979-01-01T00:00:00Z', 'success': True,
                }),
                f'kopf.zalando.org/{name2}': json.dumps({
                    'started': '1979-01-01T00:00:00Z'
                }),
            },
        },
        # 'status': {'kopf': {'progress': {
        #     name1: {'started': '1979-01-01T00:00:00Z', 'success': True},
        #     name2: {'started': '1979-01-01T00:00:00Z'},
        # }}}
    }
    event_body['metadata'] |= deletion_ts
    cause_mock.reason = cause_type

    await process_resource_event(
        lifecycle=kopf.lifecycles.one_by_one,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': event_body},
        event_queue=asyncio.Queue(),
    )

    assert extrahandlers.create_mock.call_count == (1 if cause_type == Reason.CREATE else 0)
    assert extrahandlers.update_mock.call_count == (1 if cause_type == Reason.UPDATE else 0)
    assert extrahandlers.delete_mock.call_count == (1 if cause_type == Reason.DELETE else 0)
    assert extrahandlers.resume_mock.call_count == (1 if cause_type == Reason.RESUME else 0)

    assert looptime == 0
    assert k8s_mocked.patch.called

    patch = k8s_mocked.patch.call_args_list[0].kwargs['payload']
    assert patch['metadata']['annotations'][f'kopf.zalando.org/{name1}'] is None
    assert patch['metadata']['annotations'][f'kopf.zalando.org/{name2}'] is None

    # Finalizers could be removed for resources being deleted on the 2nd step.
    # The logic can vary though: either by deletionTimestamp, or by reason==DELETE.
    if deletion_ts and deletion_ts['deletionTimestamp']:
        assert patch['metadata']['finalizers'] == []
