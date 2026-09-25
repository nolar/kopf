import pytest

from kopf._cogs.clients.errors import APIError
from kopf._cogs.clients.fetching import fetch_objs


async def test_fetching_works(kmock, settings, logger, resource, namespace):
    settings.watching.chunk_size = None
    kmock[resource, kmock.namespace(namespace)] << {'items': [{}, {}],
                                                    'metadata': {'resourceVersion': 'v1'}}

    chunks = []
    versions = []
    async for chunk, resource_version in fetch_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    ):
        chunks.append(chunk)
        versions.append(resource_version)

    assert chunks == [[{}, {}]]
    assert versions == ['v1']
    assert len(kmock['list']) == 1
    assert kmock['list', 0].params == {}  # no chunking was requested


async def test_chunking_continues(kmock, settings, logger, resource, namespace):
    settings.watching.chunk_size = 2
    kmock[resource, kmock.namespace(namespace), :1] << {'items': [{'spec': 1}, {'spec': 2}],
                                                        'metadata': {'resourceVersion': 'v1',
                                                                     'continue': 'token1'}}
    kmock[resource, kmock.namespace(namespace), :2] << {'items': [{'spec': 3}, {'spec': 4}],
                                                        'metadata': {'resourceVersion': 'v2',
                                                                     'continue': ''}}
    kmock[resource, kmock.namespace(namespace)] << {'items': [{'spec': 'unreachable'}]}

    chunks = []
    versions = []
    async for chunk, resource_version in fetch_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    ):
        chunks.append(chunk)
        versions.append(resource_version)

    # NB: versions are usually stable in K8s, but we simulate the case with different ones.
    assert versions == ['v1', 'v2']
    assert chunks == [
        [{'spec': 1}, {'spec': 2}],
        [{'spec': 3}, {'spec': 4}],
    ]
    assert len(kmock['list']) == 2
    assert kmock['list', 0].params == {'limit': '2'}
    assert kmock['list', 1].params == {'limit': '2', 'continue': 'token1'}


async def test_fetching_populates_the_missing_metadata(kmock, settings, logger, resource, namespace):
    kmock[resource, kmock.namespace(namespace)] << {'items': [{'spec': 'x'}],
                                                    'metadata': {'resourceVersion': 'v1'},
                                                    'apiVersion': 'example/v1',
                                                    'kind': 'ExampleList',
                                                    }
    chunks = []
    async for chunk, resource_version in fetch_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    ):
        chunks.append(chunk)
    assert chunks == [[{'kind': 'Example', 'apiVersion': 'example/v1', 'spec': 'x'},]]
    assert len(kmock['list']) == 1


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
# 410 comes from the expiration of the chunk continuation token (5 mins by default).
@pytest.mark.parametrize('status', [400, 403, 410, 500, 666])
async def test_raises_direct_api_errors(
        kmock, settings, logger, status, resource, namespace,
        cluster_resource, namespaced_resource):
    kmock[cluster_resource, kmock.namespace(None)] << status
    kmock[namespaced_resource, kmock.namespace('ns')] << status

    with pytest.raises(APIError) as e:
        async for _, _ in fetch_objs(
            logger=logger,
            settings=settings,
            resource=resource,
            namespace=namespace,
        ):
            pass
    assert e.value.status == status
