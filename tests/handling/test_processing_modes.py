import asyncio

import pytest

from kopf._cogs.structs.ephemera import Memo
from kopf._core.actions.lifecycles import all_at_once
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.reactor.inventory import ResourceMemories
from kopf._core.reactor.processing import process_resource_event
from kopf._core.reactor.queueing import ProcessingMode


# Just to catch the situation when new modes are added, so that we miss nothing.
# If added, they must be properly covered by the tests below.
async def test_there_are_no_unexpected_modes():
    known_modes = {ProcessingMode.INDEXING_ONLY, ProcessingMode.HANDLING_ONLY}
    extra_modes = set(ProcessingMode) - known_modes
    assert not extra_modes


@pytest.mark.parametrize('mode, exp_index, exp_event', [
    pytest.param(ProcessingMode.INDEXING_ONLY, 1, 0, id='indexing-only'),
    pytest.param(ProcessingMode.HANDLING_ONLY, 0, 1, id='handling-only'),
    pytest.param(None, 1, 1, id='regular-streaming'),
])
async def test_processing_modes(
        resource, settings, registry, handlers, mode, exp_index, exp_event):
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': 'irrelevant', 'object': {}},
        event_queue=asyncio.Queue(),
        processing_mode=mode,
    )
    assert handlers.event_mock.call_count == exp_event
    assert handlers.index_mock.call_count == exp_index
