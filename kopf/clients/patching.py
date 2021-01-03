from typing import Optional

from kopf.clients import auth, errors
from kopf.structs import bodies, patches, references


@auth.reauthenticated_request
async def patch_obj(
        *,
        resource: references.Resource,
        namespace: references.Namespace,
        name: Optional[str],
        patch: patches.Patch,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Optional[bodies.RawBody]:
    """
    Patch a resource of specific kind.

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

    as_subresource = 'status' in resource.subresources
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
            patched_body = await errors.parse_response(response)

        if status_patch:
            response = await context.session.patch(
                url=resource.get_url(server=context.server, namespace=namespace, name=name,
                                     subresource='status' if as_subresource else None),
                headers={'Content-Type': 'application/merge-patch+json'},
                json={'status': status_patch},
            )
            patched_body['status'] = (await errors.parse_response(response)).get('status')

        return patched_body

    except errors.APINotFoundError:
        return None
