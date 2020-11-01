from typing import Collection, List, Optional, Tuple

from kopf.clients import auth, errors
from kopf.structs import bodies, references


@auth.reauthenticated_request
async def list_objs_rv(
        *,
        resource: references.Resource,
        namespace: references.Namespace,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Tuple[Collection[bodies.RawBody], str]:
    """
    List the objects of specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    url = resource.get_url(server=context.server, namespace=namespace)
    rsp = await errors.parse_response(await context.session.get(url))

    items: List[bodies.RawBody] = []
    resource_version = rsp.get('metadata', {}).get('resourceVersion', None)
    for item in rsp['items']:
        if 'kind' in rsp:
            item.setdefault('kind', rsp['kind'][:-4] if rsp['kind'][-4:] == 'List' else rsp['kind'])
        if 'apiVersion' in rsp:
            item.setdefault('apiVersion', rsp['apiVersion'])
        items.append(item)

    return items, resource_version
