from typing import Optional, cast

from kopf._cogs.clients import api
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, references


async def create_obj(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace = None,
        name: Optional[str] = None,
        body: Optional[bodies.RawBody] = None,
        logger: typedefs.Logger,
) -> Optional[bodies.RawBody]:
    """
    Create a resource.
    """
    body = body if body is not None else {}
    if namespace is not None:
        body.setdefault('metadata', {}).setdefault('namespace', namespace)
    if name is not None:
        body.setdefault('metadata', {}).setdefault('name', name)

    namespace = cast(references.Namespace, body.get('metadata', {}).get('namespace'))
    created_body: bodies.RawBody = await api.post(
        url=resource.get_url(namespace=namespace),
        payload=body,
        logger=logger,
        settings=settings,
    )
    return created_body
