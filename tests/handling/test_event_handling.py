import asyncio
import logging

import pytest

import kopf
from kopf.reactor.causation import ALL_REASONS
from kopf.reactor.handling import resource_handler
from kopf.structs.containers import ResourceMemories


@pytest.mark.parametrize('cause_type', ALL_REASONS)
async def test_handlers_called_always(
        registry, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = cause_type

    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': 'ev-type', 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )

    assert handlers.event_mock.call_count == 1
    assert extrahandlers.event_mock.call_count == 1

    event = handlers.event_mock.call_args_list[0][1]['event']
    assert event['type'] == 'ev-type'
    assert event['object'] is cause_mock.body
    assert event['type'] == 'ev-type'
    assert event['object'] is cause_mock.body

    assert_logs([
        "Invoking handler 'event_fn'.",
        "Handler 'event_fn' succeeded.",
        "Invoking handler 'event_fn2'.",
        "Handler 'event_fn2' succeeded.",
    ])


@pytest.mark.parametrize('cause_type', ALL_REASONS)
async def test_errors_are_ignored(
        registry, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = cause_type
    handlers.event_mock.side_effect = Exception("oops")

    await resource_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event={'type': 'ev-type', 'object': cause_mock.body},
        freeze=asyncio.Event(),
        replenished=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )

    assert handlers.event_mock.called
    assert extrahandlers.event_mock.called

    assert_logs([
        "Invoking handler 'event_fn'.",
        "Handler 'event_fn' failed with an exception. Will ignore.",
        "Invoking handler 'event_fn2'.",
        "Handler 'event_fn2' succeeded.",
    ])
