import asyncio
import logging

import pytest

import kopf
from kopf.reactor.processing import process_resource_event
from kopf.structs.containers import ResourceMemories
from kopf.structs.handlers import Reason, HANDLER_REASONS, ALL_REASONS


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
        memories=ResourceMemories(),
        raw_event={'type': event_type, 'object': event_body},
        replenished=asyncio.Event(),
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
        memories=ResourceMemories(),
        raw_event={'type': event_type, 'object': {}},
        replenished=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )
    assert_logs([
        " event: ",
        " diff: "
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
        memories=ResourceMemories(),
        raw_event={'type': event_type, 'object': {}},
        replenished=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )
    assert_logs([
        " event: ",
    ], prohibited=[
        " diff: "
    ])
