import collections.abc

import aiohttp.web

from kopf.clients.watching import streaming_watch


async def test_watching_over_cluster(
        resp_mocker, aresponses, hostname, resource):

    list_data = {'items': [], 'metadata': {'resourceVersion': '123'}}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(list_data))
    list_url = resource.get_url(namespace=None)

    stream_query = {'watch' : 'true', 'resourceVersion': '123'}
    stream_mock = resp_mocker(return_value=aresponses.Response(text=''))
    stream_url = resource.get_url(namespace=None, params=stream_query)

    aresponses.add(hostname, list_url, 'get', list_mock, match_querystring=True)
    aresponses.add(hostname, stream_url, 'get', stream_mock, match_querystring=True)

    itr = streaming_watch(
        resource=resource,
        namespace=None,
    )

    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)

    async for _ in itr: pass  # fully deplete it

    assert list_mock.called
    assert list_mock.call_count == 1
    assert stream_mock.called
    assert stream_mock.call_count == 1


async def test_watching_over_namespace(
        resp_mocker, aresponses, hostname, resource):

    list_data = {'items': [], 'metadata': {'resourceVersion': '123'}}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(list_data))
    list_url = resource.get_url(namespace='something')

    stream_query = {'watch' : 'true', 'resourceVersion': '123'}
    stream_mock = resp_mocker(return_value=aresponses.Response(text=''))
    stream_url = resource.get_url(namespace='something', params=stream_query)

    aresponses.add(hostname, list_url, 'get', list_mock, match_querystring=True)
    aresponses.add(hostname, stream_url, 'get', stream_mock, match_querystring=True)

    itr = streaming_watch(
        resource=resource,
        namespace='something',
    )

    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)

    async for _ in itr: pass  # fully deplete it

    assert list_mock.called
    assert list_mock.call_count == 1
    assert stream_mock.called
    assert stream_mock.call_count == 1
