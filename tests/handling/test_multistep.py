import asyncio

import pytest

import kopf
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.processing import process_resource_event
from kopf.structs.containers import ResourceMemories
from kopf.structs.ephemera import Memo
from kopf.structs.handlers import HANDLER_REASONS, Reason


@pytest.mark.parametrize('deletion_ts', [
    pytest.param({}, id='no-deletion-ts'),
    pytest.param({'deletionTimestamp': None}, id='empty-deletion-ts'),
    pytest.param({'deletionTimestamp': 'some'}, id='real-deletion-ts'),
])
@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_1st_step_stores_progress_by_patching(
        registry, settings, handlers, extrahandlers,
        resource, cause_mock, cause_type, k8s_mocked, deletion_ts):
    name1 = f'{cause_type}_fn'
    name2 = f'{cause_type}_fn2'

    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    event_body = {
        'metadata': {'finalizers': [settings.persistence.finalizer]},
    }
    event_body['metadata'].update(deletion_ts)
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

    assert not k8s_mocked.sleep_or_wait.called
    assert k8s_mocked.patch_obj.called

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert patch['status']['kopf']['progress'] is not None

    assert patch['status']['kopf']['progress'][name1]['retries'] == 1
    assert patch['status']['kopf']['progress'][name1]['success'] is True

    assert patch['status']['kopf']['progress'][name2]['retries'] == 0
    assert patch['status']['kopf']['progress'][name2]['success'] is False

    assert patch['status']['kopf']['progress'][name1]['started']
    assert patch['status']['kopf']['progress'][name2]['started']

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
        resource, cause_mock, cause_type, k8s_mocked, deletion_ts):
    name1 = f'{cause_type}_fn'
    name2 = f'{cause_type}_fn2'

    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    event_body = {
        'metadata': {'finalizers': [settings.persistence.finalizer]},
        'status': {'kopf': {'progress': {
            name1: {'started': '1979-01-01T00:00:00', 'success': True},
            name2: {'started': '1979-01-01T00:00:00'},
        }}}
    }
    event_body['metadata'].update(deletion_ts)
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

    assert not k8s_mocked.sleep_or_wait.called
    assert k8s_mocked.patch_obj.called

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert patch['status']['kopf']['progress'] == {name1: None, name2: None}

    # Finalizers could be removed for resources being deleted on the 2nd step.
    # The logic can vary though: either by deletionTimestamp, or by reason==DELETE.
    if deletion_ts and deletion_ts['deletionTimestamp']:
        assert patch['metadata']['finalizers'] == []
