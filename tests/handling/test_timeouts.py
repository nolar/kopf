import asyncio
import logging

import freezegun
import pytest

import kopf
from kopf.reactor.causation import HANDLER_CAUSES
from kopf.reactor.handling import custom_object_handler


# The timeout is hard-coded in conftest.py:handlers().
# The extrahandlers are needed to prevent the cycle ending and status purging.
@pytest.mark.parametrize('cause_type', HANDLER_CAUSES)
@pytest.mark.parametrize('now, ts', [
    ['2099-12-31T23:59:59', '2020-01-01T00:00:00'],
], ids=['slow'])
async def test_timed_out_handler_fails(
        registry, handlers, extrahandlers, resource, cause_mock, cause_type,
        caplog, assert_logs, k8s_mocked, now, ts):
    caplog.set_level(logging.DEBUG)
    name1 = f'{cause_type}_fn'

    cause_mock.event = cause_type
    cause_mock.body.update({
        'status': {'kopf': {'progress': {
            'create_fn': {'started': ts},
            'update_fn': {'started': ts},
            'delete_fn': {'started': ts},
            'resume_fn': {'started': ts},
        }}}
    })

    with freezegun.freeze_time(now):
        await custom_object_handler(
            lifecycle=kopf.lifecycles.one_by_one,
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

    # Progress is reset, as the handler is not going to retry.
    assert not k8s_mocked.asyncio_sleep.called
    assert k8s_mocked.patch_obj.called

    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert patch['status']['kopf']['progress'] is not None
    assert patch['status']['kopf']['progress'][name1]['failure'] is True

    assert_logs([
        "Handler .+ failed with a fatal exception. Will stop.",
    ])
