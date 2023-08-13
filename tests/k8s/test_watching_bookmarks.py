import asyncio

from kopf._cogs.clients.watching import Bookmark, continuous_watch


async def test_listed_is_inbetween(settings, resource, namespace, kmock):

    # Resource version is used as a continuation for the watch-queries.
    kmock['list', resource, kmock.namespace(namespace)] << {
        'metadata': {'resourceVersion': '123'},
        'items': [{'spec': 'a'}, {'spec': 'b'}],
    }

    # The same as in the `stream` fixture. But here, we also mock lists.
    kmock['watch', resource, kmock.namespace(namespace), kmock.params(resourceVersion='123')] << (
        {'type': 'ADDED', 'object': {'spec': 'c'}},
        {'type': 'ADDED', 'object': {'spec': 'd'}},
        {'type': 'ERROR', 'object': {'code': 410}},
    )

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
