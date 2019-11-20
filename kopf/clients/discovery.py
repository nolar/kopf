from typing import Dict, Optional, cast

from kopf.clients import auth
from kopf.structs import resources


async def discover(
        *,
        resource: resources.Resource,
        session: auth.APISession,
) -> Optional[Dict[str, object]]:
    if resource not in session._discovered_resources:
        async with session._discovery_lock:
            if resource not in session._discovered_resources:

                response = await session.get(
                    url=resource.get_version_url(server=session.server),
                )
                response.raise_for_status()
                respdata = await response.json()

                session._discovered_resources.update({
                    resources.Resource(resource.group, resource.version, info['name']): info
                    for info in respdata['resources']
                })
    return session._discovered_resources.get(resource, None)


async def is_namespaced(
        *,
        resource: resources.Resource,
        session: auth.APISession,
) -> bool:
    info = await discover(resource=resource, session=session)
    return cast(bool, info['namespaced']) if info is not None else True  # assume namespaced
