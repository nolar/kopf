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
    """
    # Deduplicate, then sort it to make it somewhat predictable, just for the beauty of logs.
    # NB1: this also applies to v1/namespaces in the initial listing in the namespace observer.
    # NB2: it is mirrored by the same logic & query filters in the watch-streaming operation.
    label_selector = ','.join(sorted(set(settings.watching.label_selectors.collect(resource))))
    field_selector = ','.join(sorted(set(settings.watching.field_selectors.collect(resource))))

    params: dict[str, str] = {}
    if label_selector:
        params['labelSelector'] = label_selector
    if field_selector:
        params['fieldSelector'] = field_selector

    rsp = await api.get(
        url=resource.get_url(namespace=namespace, params=params),
        logger=logger,
        settings=settings,
    )

    items: list[bodies.RawBody] = []
    resource_version = rsp.get('metadata', {}).get('resourceVersion', None)
    for item in rsp.get('items', []):
        if 'kind' in rsp:
            item.setdefault('kind', rsp['kind'].removesuffix('List'))
        if 'apiVersion' in rsp:
            item.setdefault('apiVersion', rsp['apiVersion'])
        items.append(item)

    return items, resource_version
