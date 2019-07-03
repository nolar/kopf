import asyncio
import logging

import pytest

import kopf
from kopf.reactor.causation import ALL_CAUSES
from kopf.reactor.handling import custom_object_handler


@pytest.mark.parametrize('cause_type', ALL_CAUSES)
async def test_handlers_called_always(
        registry, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = cause_type

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'ev-type', 'object': cause_mock.body},
        freeze=asyncio.Event(),
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


@pytest.mark.parametrize('cause_type', ALL_CAUSES)
async def test_errors_are_ignored(
        registry, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = cause_type
    handlers.event_mock.side_effect = Exception("oops")

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'ev-type', 'object': cause_mock.body},
        freeze=asyncio.Event(),
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
