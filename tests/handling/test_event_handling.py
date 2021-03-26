import asyncio
import logging

import pytest

import kopf
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.processing import process_resource_event
from kopf.structs.containers import ResourceMemories
from kopf.structs.ephemera import Memo
from kopf.structs.handlers import ALL_REASONS


@pytest.mark.parametrize('cause_type', ALL_REASONS)
async def test_handlers_called_always(
        registry, settings, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = cause_type

    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': 'ev-type', 'object': {'field': 'value'}},
        event_queue=asyncio.Queue(),
    )

    assert handlers.event_mock.call_count == 1
    assert extrahandlers.event_mock.call_count == 1

    event = handlers.event_mock.call_args_list[0][1]['event']
    assert 'field' in event['object']
    assert event['object']['field'] == 'value'
    assert event['type'] == 'ev-type'

    assert_logs([
        "Handler 'event_fn' is invoked.",
        "Handler 'event_fn' succeeded.",
        "Handler 'event_fn2' is invoked.",
        "Handler 'event_fn2' succeeded.",
    ])


@pytest.mark.parametrize('cause_type', ALL_REASONS)
async def test_errors_are_ignored(
        registry, settings, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = cause_type
    handlers.event_mock.side_effect = Exception("oops")

    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': 'ev-type', 'object': {}},
        event_queue=asyncio.Queue(),
    )

    assert handlers.event_mock.called
    assert extrahandlers.event_mock.called

    assert_logs([
        "Handler 'event_fn' is invoked.",
        "Handler 'event_fn' failed with an exception. Will ignore.",
        "Handler 'event_fn2' is invoked.",
        "Handler 'event_fn2' succeeded.",
    ])
