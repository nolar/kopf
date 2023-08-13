import asyncio
from unittest.mock import DEFAULT

import pytest


@pytest.fixture()
def incremental_stream(kmock, resource, namespace):
    list_data = {'items': [], 'metadata': {'resourceVersion': '0'}}
    kmock['list', resource, kmock.namespace(namespace)] << list_data

    kmock['watch', resource, kmock.namespace(namespace), kmock.params(resourceVersion=0)] << (
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '1'}, 'spec': {'field': 'a'}}},
    )
    kmock['watch', resource, kmock.namespace(namespace), kmock.params(resourceVersion=1)] << (
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '2'}, 'spec': {'field': 'b'}}},
    )
    kmock['watch', resource, kmock.namespace(namespace), kmock.params(resourceVersion=2)] << (
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '3'}, 'spec': {'field': 'b'}}},
    )


@pytest.mark.usefixtures('incremental_stream', 'watcher_in_background')
async def test_consistency_tracking_in_the_watcher(settings, processor):

    # Override the default timeouts to make the tests faster.
    settings.queueing.idle_timeout = 100  # should not be involved, fail if it is
    settings.queueing.exit_timeout = 100  # should exit instantly, fail if it didn't
    settings.persistence.consistency_timeout = 3.45
    processed = asyncio.Event()

    def _processor(**_):
        nonlocal processed
        processed.set()
        return DEFAULT

    processor.side_effect = _processor

    # Stage 1: simulate the patching and setting the "expected" ResourceVersion.
    processor.return_value = '3'
    processed.clear()
    await processed.wait()
    assert processor.call_count == 1
    assert processor.call_args.kwargs['raw_event']['object']['metadata']['resourceVersion'] == '1'
    assert processor.call_args.kwargs['consistency_time'] is None

    # Stage 2: simulate an event with an INCONSISTENT ResourceVersion, which should be ignored.
    processor.return_value = None
    processed.clear()
    await processed.wait()
    assert processor.call_count == 2
    assert processor.call_args.kwargs['raw_event']['object']['metadata']['resourceVersion'] == '2'
    assert processor.call_args.kwargs['consistency_time'] == 3.45

    # Stage 3: simulate an event with a CONSISTENT ResourceVersion, which should be fully processed.
    processor.return_value = None
    processed.clear()
    await processed.wait()
    assert processor.call_count == 3
    assert processor.call_args.kwargs['raw_event']['object']['metadata']['resourceVersion'] == '3'
    assert processor.call_args.kwargs['consistency_time'] is None
