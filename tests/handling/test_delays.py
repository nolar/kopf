import asyncio
import logging

import freezegun
import pytest

import kopf
from kopf.reactor.causation import HANDLER_CAUSES, CREATE, UPDATE, DELETE, RESUME
from kopf.reactor.handling import HandlerRetryError
from kopf.reactor.handling import WAITING_KEEPALIVE_INTERVAL
from kopf.reactor.handling import custom_object_handler


@pytest.mark.parametrize('cause_type', HANDLER_CAUSES)
@pytest.mark.parametrize('now, ts, delay', [
    ['2020-01-01T00:00:00', '2020-01-01T00:04:56.789000', 4 * 60 + 56.789],
], ids=['fast'])
async def test_delayed_handlers_progress(
        registry, handlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked, now, ts, delay):
    caplog.set_level(logging.DEBUG)

    handlers.create_mock.side_effect = HandlerRetryError("oops", delay=delay)
    handlers.update_mock.side_effect = HandlerRetryError("oops", delay=delay)
    handlers.delete_mock.side_effect = HandlerRetryError("oops", delay=delay)
    handlers.resume_mock.side_effect = HandlerRetryError("oops", delay=delay)

    cause_mock.event = cause_type

    with freezegun.freeze_time(now):
        await custom_object_handler(
            lifecycle=kopf.lifecycles.all_at_once,
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

    fname = f'{cause_type}_fn'
    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert patch['status']['kopf']['progress'][fname]['delayed'] == ts

    assert_logs([
        "Invoking handler .+",
        "Handler .+ failed with a retry exception. Will retry.",
    ])


@pytest.mark.parametrize('cause_type', HANDLER_CAUSES)
@pytest.mark.parametrize('now, ts, delay', [
    ['2020-01-01T00:00:00', '2020-01-01T00:04:56.789000', 4 * 60 + 56.789],
    ['2020-01-01T00:00:00', '2099-12-31T23:59:59.000000', WAITING_KEEPALIVE_INTERVAL],
], ids=['fast', 'slow'])
async def test_delayed_handlers_sleep(
        registry, handlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked, now, ts, delay):
    caplog.set_level(logging.DEBUG)

    cause_mock.event = cause_type
    cause_mock.body.update({
        'status': {'kopf': {'progress': {
            'create_fn': {'delayed': ts},
            'update_fn': {'delayed': ts},
            'delete_fn': {'delayed': ts},
            'resume_fn': {'delayed': ts},
        }}}
    })

    with freezegun.freeze_time(now):
        await custom_object_handler(
            lifecycle=kopf.lifecycles.all_at_once,
            registry=registry,
            resource=resource,
            event={'type': 'irrelevant', 'object': cause_mock.body},
            freeze=asyncio.Event(),
            event_queue=asyncio.Queue(),
        )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called
    assert not handlers.resume_mock.called

    # The dummy patch is needed to trigger the further changes. The value is irrelevant.
    assert k8s_mocked.patch_obj.called
    assert 'dummy' in k8s_mocked.patch_obj.call_args_list[0][1]['patch']['status']['kopf']

    # The duration of sleep should be as expected.
    assert k8s_mocked.asyncio_sleep.called
    assert k8s_mocked.asyncio_sleep.call_args_list[0][0][0] == delay

    assert_logs([
        "Sleeping for [\d\.]+ seconds",
    ])
