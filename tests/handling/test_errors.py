import asyncio
import logging

import pytest

import kopf
from kopf.reactor.causation import HANDLER_CAUSES, CREATE, UPDATE, DELETE, RESUME
from kopf.reactor.handling import HandlerFatalError, HandlerRetryError
from kopf.reactor.handling import custom_object_handler


# The extrahandlers are needed to prevent the cycle ending and status purging.
@pytest.mark.parametrize('cause_type', HANDLER_CAUSES)
async def test_fatal_error_stops_handler(
        registry, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    name1 = f'{cause_type}_fn'

    cause_mock.event = cause_type
    handlers.create_mock.side_effect = HandlerFatalError("oops")
    handlers.update_mock.side_effect = HandlerFatalError("oops")
    handlers.delete_mock.side_effect = HandlerFatalError("oops")
    handlers.resume_mock.side_effect = HandlerFatalError("oops")

    await custom_object_handler(
        lifecycle=kopf.lifecycles.one_by_one,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )

    assert handlers.create_mock.call_count == (1 if cause_type == CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_type == UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_type == DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_type == RESUME else 0)

    assert not k8s_mocked.asyncio_sleep.called
    assert k8s_mocked.patch_obj.called

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert patch['status']['kopf']['progress'] is not None
    assert patch['status']['kopf']['progress'][name1]['failure'] is True
    assert patch['status']['kopf']['progress'][name1]['message'] == 'oops'

    assert_logs([
        "Handler .+ failed with a fatal exception. Will stop.",
    ])


# The extrahandlers are needed to prevent the cycle ending and status purging.
@pytest.mark.parametrize('cause_type', HANDLER_CAUSES)
async def test_retry_error_delays_handler(
        registry, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    name1 = f'{cause_type}_fn'

    cause_mock.event = cause_type
    handlers.create_mock.side_effect = HandlerRetryError("oops")
    handlers.update_mock.side_effect = HandlerRetryError("oops")
    handlers.delete_mock.side_effect = HandlerRetryError("oops")
    handlers.resume_mock.side_effect = HandlerRetryError("oops")

    await custom_object_handler(
        lifecycle=kopf.lifecycles.one_by_one,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )

    assert handlers.create_mock.call_count == (1 if cause_type == CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_type == UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_type == DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_type == RESUME else 0)

    assert not k8s_mocked.asyncio_sleep.called
    assert k8s_mocked.patch_obj.called

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert patch['status']['kopf']['progress'] is not None
    assert 'failure' not in patch['status']['kopf']['progress'][name1]
    assert 'success' not in patch['status']['kopf']['progress'][name1]
    assert 'delayed' in patch['status']['kopf']['progress'][name1]

    assert_logs([
        "Handler .+ failed with a retry exception. Will retry.",
    ])


# The extrahandlers are needed to prevent the cycle ending and status purging.
@pytest.mark.parametrize('cause_type', HANDLER_CAUSES)
async def test_arbitrary_error_delays_handler(
        registry, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked):
    caplog.set_level(logging.DEBUG)
    name1 = f'{cause_type}_fn'

    cause_mock.event = cause_type
    handlers.create_mock.side_effect = Exception("oops")
    handlers.update_mock.side_effect = Exception("oops")
    handlers.delete_mock.side_effect = Exception("oops")
    handlers.resume_mock.side_effect = Exception("oops")

    await custom_object_handler(
        lifecycle=kopf.lifecycles.one_by_one,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )

    assert handlers.create_mock.call_count == (1 if cause_type == CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_type == UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_type == DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_type == RESUME else 0)

    assert not k8s_mocked.asyncio_sleep.called
    assert k8s_mocked.patch_obj.called

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert patch['status']['kopf']['progress'] is not None
    assert 'failure' not in patch['status']['kopf']['progress'][name1]
    assert 'success' not in patch['status']['kopf']['progress'][name1]
    assert 'delayed' in patch['status']['kopf']['progress'][name1]

    assert_logs([
        "Handler .+ failed with an exception. Will retry.",
    ])
