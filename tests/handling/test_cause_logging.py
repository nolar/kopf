import asyncio
import logging

import pytest

import kopf
from kopf.reactor.causation import ALL_CAUSES
from kopf.reactor.handling import custom_object_handler


@pytest.mark.parametrize('cause_type', ALL_CAUSES)
async def test_all_logs_are_prefixed(registry, resource, caplog, cause_type, cause_mock):
    caplog.set_level(logging.DEBUG)
    cause_mock.event = cause_type

    await custom_object_handler(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        resource=resource,
        event={'type': 'irrelevant', 'object': cause_mock.body},
        freeze=asyncio.Event(),
    )
    assert all(message.startswith('[ns1/name1] ') for message in caplog.messages)
