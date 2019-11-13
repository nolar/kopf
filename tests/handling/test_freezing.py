import asyncio
import logging

import kopf
from kopf.reactor.handling import resource_handler
from kopf.reactor.registries import OperatorRegistry
from kopf.structs.containers import ResourceMemories


async def test_nothing_is_called_when_freeze_is_set(mocker, resource, caplog, assert_logs):
    detect_cause = mocker.patch('kopf.reactor.causation.detect_resource_changing_cause')
    handle_cause = mocker.patch('kopf.reactor.handling.handle_resource_changing_cause')
    patch_obj = mocker.patch('kopf.clients.patching.patch_obj')
    asyncio_sleep = mocker.patch('asyncio.sleep')

    # Nothing of these is actually used, but we need to feed something.
    # Except for namespace+name, which are used for the logger prefixes.
    lifecycle = kopf.lifecycles.all_at_once
    registry = OperatorRegistry()
    event = {'object': {'metadata': {'namespace': 'ns1', 'name': 'name1'}}}

    # This is what makes it frozen.
    freeze = asyncio.Event()
    freeze.set()

    caplog.set_level(logging.DEBUG)
    await resource_handler(
        lifecycle=lifecycle,
        registry=registry,
        resource=resource,
        memories=ResourceMemories(),
        event=event,
        freeze=freeze,
        replenished=asyncio.Event(),
        event_queue=asyncio.Queue(),
    )

    assert not detect_cause.called
    assert not handle_cause.called
    assert not patch_obj.called
    assert not asyncio_sleep.called

    assert_logs([
        r"Ignoring the events due to freeze.",
    ])
