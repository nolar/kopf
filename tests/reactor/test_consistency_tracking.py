import asyncio
import math
from unittest.mock import DEFAULT

import pytest


@pytest.fixture()
def incremental_stream(hostname, aresponses, resource, namespace):
    list_data = {'items': [], 'metadata': {'resourceVersion': '0'}}
    list_url = resource.get_url(namespace=namespace)
    aresponses.add(hostname, list_url, 'get', list_data, match_querystring=True, repeat=math.inf)

    url0 = resource.get_url(namespace=namespace, params={'watch': 'true', 'resourceVersion': '0'})
    aresponses.add(
        hostname, url0, 'get',
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '1'}, 'spec': {'field': 'a'}}},
        match_querystring=True,
    )

    url1 = resource.get_url(namespace=namespace, params={'watch': 'true', 'resourceVersion': '1'})
    aresponses.add(
        hostname, url1, 'get',
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '2'}, 'spec': {'field': 'b'}}},
        match_querystring=True,
    )

    url2 = resource.get_url(namespace=namespace, params={'watch': 'true', 'resourceVersion': '2'})
    aresponses.add(
        hostname, url2, 'get',
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '3'}, 'spec': {'field': 'b'}}},
        match_querystring=True,
    )


@pytest.mark.usefixtures('incremental_stream', 'watcher_in_background')
async def test_consistency_tracking_in_the_watcher(
        settings, caplog, processor):

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
    assert processor.call_args[1]['raw_event']['object']['metadata']['resourceVersion'] == '1'
    assert processor.call_args[1]['consistency_time'] is None

    # Stage 2: simulate an event with an INCONSISTENT ResourceVersion, which should be ignored.
    processor.return_value = None
    processed.clear()
    await processed.wait()
    assert processor.call_count == 2
    assert processor.call_args[1]['raw_event']['object']['metadata']['resourceVersion'] == '2'
    assert processor.call_args[1]['consistency_time'] == 3.45

    # Stage 3: simulate an event with a CONSISTENT ResourceVersion, which should be fully processed.
    processor.return_value = None
    processed.clear()
    await processed.wait()
    assert processor.call_count == 3
    assert processor.call_args[1]['raw_event']['object']['metadata']['resourceVersion'] == '3'
    assert processor.call_args[1]['consistency_time'] is None
