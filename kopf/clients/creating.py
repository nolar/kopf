from typing import Optional, cast

from kopf.clients import auth, errors
from kopf.structs import bodies, references


@auth.reauthenticated_request
async def create_obj(
        *,
        resource: references.Resource,
        namespace: references.Namespace = None,
        name: Optional[str] = None,
        body: Optional[bodies.RawBody] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Optional[bodies.RawBody]:
    """
    Create a resource.
    """
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    body = body if body is not None else {}
    if namespace is not None:
        body.setdefault('metadata', {}).setdefault('namespace', namespace)
    if name is not None:
        body.setdefault('metadata', {}).setdefault('name', name)

    namespace = cast(references.Namespace, body.get('metadata', {}).get('namespace'))
    response = await context.session.post(
        url=resource.get_url(server=context.server, namespace=namespace),
        json=body,
    )
    created_body: bodies.RawBody = await errors.parse_response(response)
    return created_body
