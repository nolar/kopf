from typing import Optional, cast

import aiohttp

from kopf.clients import auth
from kopf.clients import discovery
from kopf.structs import bodies
from kopf.structs import patches
from kopf.structs import resources


@auth.reauthenticated_request
async def patch_obj(
        *,
        resource: resources.Resource,
        patch: patches.Patch,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        body: Optional[bodies.Body] = None,
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> None:
    """
    Patch a resource of specific kind.

    Either the namespace+name should be specified, or the body,
    which is used only to get namespace+name identifiers.

    Unlike the object listing, the namespaced call is always
    used for the namespaced resources, even if the operator serves
    the whole cluster (i.e. is not namespace-restricted).
    """
    if session is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    if body is not None and (name is not None or namespace is not None):
        raise TypeError("Either body, or name+namespace can be specified. Got both.")

    namespace = body.get('metadata', {}).get('namespace') if body is not None else namespace
    name = body.get('metadata', {}).get('name') if body is not None else name

    is_namespaced = await discovery.is_namespaced(resource=resource, session=session)
    namespace = namespace if is_namespaced else None

    if body is None:
        body = cast(bodies.Body, {'metadata': {'name': name}})
        if namespace is not None:
            body['metadata']['namespace'] = namespace

    try:
        await session.patch(
            url=resource.get_url(server=session.server, namespace=namespace, name=name),
            headers={'Content-Type': 'application/merge-patch+json'},
            json=patch,
            raise_for_status=True,
        )
    except aiohttp.ClientResponseError as e:
        if e.status == 404:
            pass
        else:
            raise
