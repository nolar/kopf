from collections.abc import AsyncIterator, Collection

from kopf._cogs.clients import api
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, references


async def fetch_objs(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        logger: typedefs.Logger,
) -> AsyncIterator[tuple[Collection[bodies.RawBody], str]]:
    """
    Fetch the objects of specific resource type in chunks.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes no sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.

    It yields an individual chunk as soon as it retrieves it from the API.
    The consumer should process the chunk before going to the next one —
    to optimize the memory usage, i.e. so that it never accumulates and keeps
    all existing objects in memory altogether. At most keep one chunk.

    Chunking also applies to the observer, i.e. the CRD and namespace lists.
    It is rare to see clusters with so many resources or namespaces
    that it requires chunking, but this is not entirely impossible.

    The resource version is stable in all chunks and is yielded that way
    only to simplify the type annotations and internal data structures.
    """
    chunk_size: int | None = settings.watching.chunk_size
    iterations = 0
    continue_token: str = ''  # NB: it becomes an empty string at the end, not null!
    while not iterations or continue_token:
        iterations += 1

        params: dict[str, str] = {}
        if chunk_size is not None:  # pass 0 through to the api; not our business
            params['limit'] = str(chunk_size)
        if continue_token:
            params['continue'] = continue_token

        rsp = await api.get(
            url=resource.get_url(namespace=namespace, params=params),
            logger=logger,
            settings=settings,
        )

        # Adjust items for missing individual metadata from the same metadata of the list.
        items: list[bodies.RawBody] = rsp.get('items', [])
        for item in items:
            if 'kind' in rsp:
                item.setdefault('kind', rsp['kind'].removesuffix('List'))
            if 'apiVersion' in rsp:
                item.setdefault('apiVersion', rsp['apiVersion'])

        # NB: this is the resource version of the list/chunk, not of an individual item.
        resource_version: str = rsp.get('metadata', {}).get('resourceVersion')
        continue_token = rsp.get('metadata', {}).get('continue')  # NB: "" at the end, not None!
        yield items, resource_version

        # Optimization: do not keep the processed chunk in memory before fetching the next one,
        # so that at any moment in time we keep at most one chunk in memory, not two (old & new).
        # Unlike the individual objects (not optimized), chunks can be huge.
        # This is untestable due to garbage collection internals.
        # Does not work in PyPy due to delayed gc.
        del items, rsp
