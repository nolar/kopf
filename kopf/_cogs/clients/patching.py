from kopf._cogs.clients import api, errors
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, patches, references


async def patch_obj(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        name: str | None,
        patch: patches.Patch,
        logger: typedefs.Logger,
) -> tuple[bodies.RawBody | None, patches.Patch | None]:
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
    as_subresource = 'status' in resource.subresources
    body_patch = dict(patch)  # shallow: for mutation of the top-level keys below.
    status_patch = body_patch.pop('status', None) if as_subresource else None
    status_patch = {'status': status_patch} if status_patch is not None else None

    # Patch & reconstruct the actual body as reported by the server. The reconstructed body can be
    # partial or empty -- if the body/status patches are empty. This is fine: it is only used
    # to verify that the patched fields are matching the patch. No patch? No mismatch!
    try:
        patched_body: bodies.RawBody | None = None

        if body_patch:
            logger.debug(f"Merge-patching the resource with: {body_patch!r}")
            patched_body = await api.patch(
                url=resource.get_url(namespace=namespace, name=name),
                headers={'Content-Type': 'application/merge-patch+json'},
                payload=body_patch,
                settings=settings,
                logger=logger,
            )

        if status_patch:
            # NB: we need the new resourceVersion, so we take the whole new patched body.
            logger.debug(f"Merge-patching the status with: {status_patch!r}")
            patched_body = await api.patch(
                url=resource.get_url(namespace=namespace, name=name, subresource='status'),
                headers={'Content-Type': 'application/merge-patch+json'},
                payload=status_patch,
                settings=settings,
                logger=logger,
            )

        # Only the callable transformations are left now, no dict-merge components left.
        # If we fail at any stage below, we re-apply this whole patch. We cannot distinguish
        # the "status" functions from the "main body" functions; some of them can be mixed.
        remaining_patch = patches.Patch(fns=patch.fns)

        # Calculate the JSON diffs to be applied and split into body + status (if a subresource).
        # Use the latest known body for reference when calculating the item indexes in lists.
        # Note: we apply transformations for the whole body atomically, and in the future (not yet)
        # we want to get the ops from the functions, so we can only filter by path here.
        fresh_body: bodies.RawBody | None = patched_body if patched_body else patch._original
        ops: patches.JSONPatch = remaining_patch.as_json_patch(fresh_body)
        body_ops: patches.JSONPatch
        status_ops: patches.JSONPatch
        if as_subresource:
            body_ops = [op for op in ops
                        if not(op['path'] == '/status' or op['path'].startswith('/status/'))]
            status_ops = [op for op in ops
                          if op['path'] == '/status' or op['path'].startswith('/status/')]
        else:
            body_ops = ops
            status_ops = []

        # Apply pending operations (finalizers) via JSON-patch with optimistic concurrency.
        # If the resource version fails the test, we know there are newer changes pending,
        # so we will get back to the handling cycle & this same patching almost immediately.
        # NB: the patched_body=={} in tests' mocks, so check for emptiness, not just null-ness.
        if body_ops:
            resource_version = (fresh_body or {}).get('metadata', {}).get('resourceVersion')
            test = patches.JSONPatchItem(op='test', path='/metadata/resourceVersion', value=resource_version)
            ops = [test] + body_ops
            logger.debug(f"JSON-patching the resource with: {body_ops!r}")
            try:
                patched_body = await api.patch(
                    url=resource.get_url(namespace=namespace, name=name),
                    headers={'Content-Type': 'application/json-patch+json'},
                    payload=ops,
                    settings=settings,
                    logger=logger,
                )
            except errors.APIUnprocessableEntityError:
                # NB: also detach from the current freshest body, persist only the patch fns.
                logger.debug(
                    "Could not apply the patch in full due conflicts with newer changes. "
                    f"Will try on the next cycle soon. Remaining: {remaining_patch!r}"
                )
                return patched_body, remaining_patch
            else:
                fresh_body = patched_body

        # We DO NOT recalculate the diff against a newer body after the recent patch!
        # The ops of both diffs are already non-conflicting, because we have split them so.
        # If the body patch succeeds, we know the new fresh state. If it fails, we do not get here.
        # If something jumps inbetween, we fail & postpone the whole JSON-patch to the next cycle.
        if status_ops:
            resource_version = (fresh_body or {}).get('metadata', {}).get('resourceVersion')
            test = patches.JSONPatchItem(op='test', path='/metadata/resourceVersion', value=resource_version)
            ops = [test] + status_ops
            logger.debug(f"JSON-patching the status with: {status_ops!r}")
            try:
                patched_body = await api.patch(
                    url=resource.get_url(namespace=namespace, name=name, subresource='status'),
                    headers={'Content-Type': 'application/json-patch+json'},
                    payload=ops,
                    settings=settings,
                    logger=logger,
                )
            except errors.APIUnprocessableEntityError:
                # NB: also detach from the current freshest body, persist only the patch fns.
                logger.debug(
                    "Could not apply the patch in full due conflicts with newer changes. "
                    f"Will try on the next cycle soon. Remaining: {remaining_patch!r}"
                )
                return patched_body, remaining_patch

        return patched_body, None

    except errors.APINotFoundError:
        logger.debug(f"Patching was skipped: the object does not exist anymore.")
        return None, None
