from collections.abc import Collection

from kopf._cogs.clients import api
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, references


async def list_objs(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        logger: typedefs.Logger,
) -> tuple[Collection[bodies.RawBody], str]:
    """
    List the objects of specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes no sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.

    When ``settings.watching.batch_size`` is set, the collection is fetched in
    chunks via the Kubernetes API's ``limit``/``continue`` pagination, which
    keeps the peak memory footprint low for large collections. Otherwise, the
    whole collection is fetched in a single request (the default behaviour).
    """
    items: list[bodies.RawBody] = []
    resource_version: str | None = None
    continue_token: str | None = None
    while True:

        params: dict[str, str] = {}
        if settings.watching.batch_size is not None:
            params['limit'] = str(settings.watching.batch_size)
        if continue_token:
            params['continue'] = continue_token

        rsp = await api.get(
            url=resource.get_url(namespace=namespace, params=params or None),
            logger=logger,
            settings=settings,
        )

        # All chunks of a paginated list share one consistent snapshot, so the
        # resource version is the same on every page; keep the first non-empty
        # one to continue the watch-stream from the fully listed state.
        if resource_version is None:
            resource_version = rsp.get('metadata', {}).get('resourceVersion', None)

        for item in rsp.get('items', []):
            if 'kind' in rsp:
                item.setdefault('kind', rsp['kind'].removesuffix('List'))
            if 'apiVersion' in rsp:
                item.setdefault('apiVersion', rsp['apiVersion'])
            items.append(item)

        # Keep fetching while the server reports more chunks of the collection.
        continue_token = rsp.get('metadata', {}).get('continue', None)
        if not continue_token:
            break

    return items, resource_version
