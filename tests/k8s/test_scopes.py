import collections.abc

import pytest

from kopf.clients.watching import streaming_watch


class PreventedActualCallError(Exception):
    pass


async def test_watching_over_cluster(resource, req_mock):
    req_mock.get.side_effect = PreventedActualCallError()

    itr = streaming_watch(
        resource=resource,
        namespace=None,
    )

    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)

    with pytest.raises(PreventedActualCallError):
        async for _ in itr: pass  # fully deplete it

    assert req_mock.get.called
    assert req_mock.get.call_count == 1

    url = req_mock.get.call_args_list[0][1]['url']
    assert 'apis/zalando.org/v1/kopfexamples' in url
    assert 'namespaces/' not in url


async def test_watching_over_namespace(resource, req_mock):
    req_mock.get.side_effect = PreventedActualCallError()

    itr = streaming_watch(
        resource=resource,
        namespace='something',
    )

    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)

    with pytest.raises(PreventedActualCallError):
        async for _ in itr: pass  # fully deplete it

    assert req_mock.get.called
    assert req_mock.get.call_count == 1

    url = req_mock.get.call_args_list[0][1]['url']
    assert 'apis/zalando.org/v1/namespaces/something/kopfexamples' in url
