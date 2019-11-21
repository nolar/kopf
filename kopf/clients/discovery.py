from typing import Dict, Optional, cast

import aiohttp

from kopf.clients import auth
from kopf.structs import resources


@auth.reauthenticated_request
async def discover(
        *,
        resource: resources.Resource,
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> Optional[Dict[str, object]]:
    if session is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    if resource.api_version not in session._discovered_resources:
        async with session._discovery_lock:
            if resource.api_version not in session._discovered_resources:
                session._discovered_resources[resource.api_version] = {}

                try:
                    response = await session.get(
                        url=resource.get_version_url(server=session.server),
                    )
                    response.raise_for_status()
                    respdata = await response.json()

                    session._discovered_resources[resource.api_version].update({
                        resources.Resource(resource.group, resource.version, info['name']): info
                        for info in respdata['resources']
                    })

                except aiohttp.ClientResponseError as e:
                    if e.status in [403, 404]:
                        pass
                    else:
                        raise

    return session._discovered_resources[resource.api_version].get(resource, None)


@auth.reauthenticated_request
async def is_namespaced(
        *,
        resource: resources.Resource,
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> bool:
    if session is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    info = await discover(resource=resource, session=session)
    return cast(bool, info['namespaced']) if info is not None else True  # assume namespaced
