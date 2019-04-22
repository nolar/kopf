import collections.abc

from asynctest import call

from kopf.reactor.watching import streaming_watch

EMPTY_EVENT_STREAM = []


async def test_watching_over_cluster(resource, stream_fn, apicls_fn):
    apicls_fn.return_value.list_cluster_custom_object = object()
    stream_fn.return_value = iter(EMPTY_EVENT_STREAM)

    itr = streaming_watch(
        resource=resource,
        namespace=None,
    )

    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)
    async for _ in itr: pass  # fully deplete it

    assert stream_fn.called
    assert stream_fn.call_count == 1
    assert stream_fn.call_args_list == [
        call(apicls_fn.return_value.list_cluster_custom_object,
             group=resource.group, version=resource.version, plural=resource.plural),
    ]


async def test_watching_over_namespace(resource, stream_fn, apicls_fn):
    apicls_fn.return_value.list_namespaced_custom_object = object()
    stream_fn.return_value = iter(EMPTY_EVENT_STREAM)

    itr = streaming_watch(
        resource=resource,
        namespace='something',
    )

    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)
    async for _ in itr: pass  # fully deplete it

    assert stream_fn.called
    assert stream_fn.call_count == 1
    assert stream_fn.call_args_list == [
        call(apicls_fn.return_value.list_namespaced_custom_object,
             group=resource.group, version=resource.version, plural=resource.plural,
             namespace='something'),
    ]
