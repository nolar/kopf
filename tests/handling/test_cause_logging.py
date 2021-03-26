import asyncio
import datetime
import logging

import freezegun
import pytest

import kopf
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.processing import process_resource_event
from kopf.storage.progress import StatusProgressStorage
from kopf.structs.containers import ResourceMemories
from kopf.structs.ephemera import Memo
from kopf.structs.handlers import ALL_REASONS, HANDLER_REASONS, Reason


@pytest.mark.parametrize('cause_type', ALL_REASONS)
async def test_all_logs_are_prefixed(registry, settings, resource, handlers,
                                     logstream, cause_type, cause_mock):
    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    event_body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
    cause_mock.reason = cause_type

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

    lines = logstream.getvalue().splitlines()
    assert lines  # no messages means that we cannot test it
    assert all(line.startswith('prefix [ns1/name1] ') for line in lines)


@pytest.mark.parametrize('diff', [
    pytest.param((('op', ('field',), 'old', 'new'),), id='realistic-diff'),
])
@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_diffs_logged_if_present(registry, settings, resource, handlers,
                                       cause_type, cause_mock, caplog, assert_logs, diff):
    caplog.set_level(logging.DEBUG)

    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_type
    cause_mock.diff = diff
    cause_mock.new = {'field': 'old'}  # checked for `not None`, and JSON-serialised
    cause_mock.old = {'field': 'new'}  # checked for `not None`, and JSON-serialised

    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=asyncio.Queue(),
    )
    assert_logs([
        "(Creation|Updating|Resuming|Deletion) is in progress: ",
        "(Creation|Updating|Resuming|Deletion) diff: "
    ])


@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
@pytest.mark.parametrize('diff', [
    pytest.param(None, id='none-diff'),  # same as the default, but for clarity
    pytest.param([], id='empty-list-diff'),
    pytest.param((), id='empty-tuple-diff'),
])
async def test_diffs_not_logged_if_absent(registry, settings, resource, handlers, cause_type, cause_mock,
                                          caplog, assert_logs, diff):
    caplog.set_level(logging.DEBUG)

    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_type
    cause_mock.diff = diff

    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=asyncio.Queue(),
    )
    assert_logs([
        "(Creation|Updating|Resuming|Deletion) is in progress: ",
    ], prohibited=[
        " diff: "
    ])



# Timestamps: time zero (0), before (B), after (A), and time zero+1s (1).
TS0 = datetime.datetime(2020, 12, 31, 23, 59, 59, 123456)
TS1_ISO = '2021-01-01T00:00:00.123456'


@pytest.mark.parametrize('cause_types', [
    # All combinations except for same-to-same (it is not an "extra" then).
    (a, b) for a in HANDLER_REASONS for b in HANDLER_REASONS if a != b
])
@freezegun.freeze_time(TS0)
async def test_supersession_is_logged(
        registry, settings, resource, handlers, cause_types, cause_mock, caplog, assert_logs):
    caplog.set_level(logging.DEBUG)

    settings.persistence.progress_storage = StatusProgressStorage()
    body = {'status': {'kopf': {'progress': {
        'create_fn': {'purpose': cause_types[0]},
        'update_fn': {'purpose': cause_types[0]},
        'resume_fn': {'purpose': cause_types[0]},
        'delete_fn': {'purpose': cause_types[0]},
    }}}}

    cause_mock.reason = cause_types[1]
    event_type = None if cause_types[1] == Reason.RESUME else 'irrelevant'

    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
    )
    assert_logs([
        "(Creation|Updating|Resuming|Deletion) is superseded by (creation|updating|resuming|deletion): ",
        "(Creation|Updating|Resuming|Deletion) is in progress: ",
        "(Creation|Updating|Resuming|Deletion) is processed: ",
    ])
