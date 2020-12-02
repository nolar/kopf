from typing import Optional, cast

import aiohttp

from kopf.clients import auth, discovery, errors
from kopf.structs import bodies, patches, resources


@auth.reauthenticated_request
async def patch_obj(
        *,
        resource: resources.Resource,
        patch: patches.Patch,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        body: Optional[bodies.Body] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Optional[bodies.RawBody]:
    """
    Patch a resource of specific kind.

    Either the namespace+name should be specified, or the body,
    which is used only to get namespace+name identifiers.

    Unlike the object listing, the namespaced call is always
    used for the namespaced resources, even if the operator serves
    the whole cluster (i.e. is not namespace-restricted).

    Returns the patched body. The patched body can be partial (status-only,
    no-status, or empty) -- depending on whether there were fields in the body
    or in the status to patch; if neither had fields for patching, the result
    is an empty body. The result should only be used to check against the patch:
    if there was nothing to patch, it does not matter if the fields are absent.

    Returns ``None`` if the underlying object is absent, as detected by trying
    to patch it and failing with HTTP 404. This can happen if the object was
    deleted in the operator's handlers or externally during the processing,
    so that the framework was unaware of these changes until the last moment.
    """
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    if body is not None and (name is not None or namespace is not None):
        raise TypeError("Either body, or name+namespace can be specified. Got both.")

    namespace = body.get('metadata', {}).get('namespace') if body is not None else namespace
    name = body.get('metadata', {}).get('name') if body is not None else name

    is_namespaced = await discovery.is_namespaced(resource=resource, context=context)
    namespace = namespace if is_namespaced else None

    if body is None:
        body = cast(bodies.Body, {'metadata': {'name': name}})
        if namespace is not None:
            body['metadata']['namespace'] = namespace

    as_subresource = await discovery.is_status_subresource(resource=resource, context=context)
    body_patch = dict(patch)  # shallow: for mutation of the top-level keys below.
    status_patch = body_patch.pop('status', None) if as_subresource else None

    # Patch & reconstruct the actual body as reported by the server. The reconstructed body can be
    # partial or empty -- if the body/status patches are empty. This is fine: it is only used
    # to verify that the patched fields are matching the patch. No patch? No mismatch!
    try:
        patched_body = bodies.RawBody()

        if body_patch:
            response = await context.session.patch(
                url=resource.get_url(server=context.server, namespace=namespace, name=name),
                headers={'Content-Type': 'application/merge-patch+json'},
                json=body_patch,
            )
            await errors.check_response(response)
            patched_body = await response.json()

        if status_patch:
            response = await context.session.patch(
                url=resource.get_url(server=context.server, namespace=namespace, name=name,
                                     subresource='status' if as_subresource else None),
                headers={'Content-Type': 'application/merge-patch+json'},
                json={'status': status_patch},
            )
            await errors.check_response(response)
            patched_body['status'] = (await response.json()).get('status')

        return patched_body

    except aiohttp.ClientResponseError as e:
        if e.status == 404:
            return None
        else:
            raise
