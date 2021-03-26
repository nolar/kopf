import asyncio
import json
import logging

import aiohttp.web
import pytest

from kopf.clients.watching import Bookmark, continuous_watch


async def test_listed_is_inbetween(
        settings, resource, namespace, hostname, aresponses):

    # Resource version is used as a continutation for the watch-queries.
    list_data = {'metadata': {'resourceVersion': '123'}, 'items': [
        {'spec': 'a'},
        {'spec': 'b'},
    ]}
    list_resp = aiohttp.web.json_response(list_data)
    list_url = resource.get_url(namespace=namespace)

    # The same as in the `stream` fixture. But here, we also mock lists.
    stream_data = [
        {'type': 'ADDED', 'object': {'spec': 'c'}},  # stream.feed()
        {'type': 'ADDED', 'object': {'spec': 'd'}},  # stream.feed()
        {'type': 'ERROR', 'object': {'code': 410}},  # stream.close()
    ]
    stream_text = '\n'.join(json.dumps(event) for event in stream_data)
    stream_resp = aresponses.Response(text=stream_text)
    stream_query = {'watch': 'true', 'resourceVersion': '123'}
    stream_url = resource.get_url(namespace=namespace, params=stream_query)

    aresponses.add(hostname, list_url, 'get', list_resp, match_querystring=True)
    aresponses.add(hostname, stream_url, 'get', stream_resp, match_querystring=True)

    events = []
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 5
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'b'
    assert events[2] == Bookmark.LISTED
    assert events[3]['object']['spec'] == 'c'
    assert events[4]['object']['spec'] == 'd'
