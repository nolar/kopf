from typing import Dict, Optional, cast

import aiohttp

from kopf.clients import auth
from kopf.structs import resources


@auth.reauthenticated_request
async def discover(
        *,
        resource: resources.Resource,
        subresource: Optional[str] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Optional[Dict[str, object]]:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    if resource.api_version not in context._discovered_resources:
        async with context._discovery_lock:
            if resource.api_version not in context._discovered_resources:
                context._discovered_resources[resource.api_version] = {}

                try:
                    response = await context.session.get(
                        url=resource.get_version_url(server=context.server),
                    )
                    response.raise_for_status()
                    respdata = await response.json()

                    context._discovered_resources[resource.api_version].update({
                        info['name']: info
                        for info in respdata['resources']
                    })

                except aiohttp.ClientResponseError as e:
                    if e.status in [403, 404]:
                        pass
                    else:
                        raise

    name = resource.plural if subresource is None else f'{resource.plural}/{subresource}'
    return context._discovered_resources[resource.api_version].get(name, None)


@auth.reauthenticated_request
async def is_namespaced(
        *,
        resource: resources.Resource,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> bool:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    info = await discover(resource=resource, context=context)
    return cast(bool, info['namespaced']) if info is not None else True  # assume namespaced


@auth.reauthenticated_request
async def is_status_subresource(
        *,
        resource: resources.Resource,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> bool:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    info = await discover(resource=resource, subresource='status', context=context)
    return info is not None
