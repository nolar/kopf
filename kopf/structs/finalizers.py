"""
All the functions to manipulate the object finalization and deletion.

Finalizers are used to block the actual deletion until the finalizers
are removed, meaning that the operator has done all its duties
to "release" the object (e.g. cleanups; delete-handlers in our case).
"""
from kopf.structs import bodies
from kopf.structs import patches

# A string marker to be put on the list of the finalizers to block
# the object from being deleted without the permission of the framework.
FINALIZER = 'kopf.zalando.org/KopfFinalizerMarker'
LEGACY_FINALIZER = 'KopfFinalizerMarker'


def is_deleted(
        body: bodies.Body,
) -> bool:
    return body.get('metadata', {}).get('deletionTimestamp', None) is not None


def has_finalizers(
        body: bodies.Body,
) -> bool:
    finalizers = body.get('metadata', {}).get('finalizers', [])
    return FINALIZER in finalizers or LEGACY_FINALIZER in finalizers


def append_finalizers(
        *,
        body: bodies.Body,
        patch: patches.Patch,
) -> None:
    if not has_finalizers(body=body):
        finalizers = body.get('metadata', {}).get('finalizers', [])
        patch.setdefault('metadata', {}).setdefault('finalizers', list(finalizers))
        patch['metadata']['finalizers'].append(FINALIZER)


def remove_finalizers(
        *,
        body: bodies.Body,
        patch: patches.Patch,
) -> None:
    if has_finalizers(body=body):
        finalizers = body.get('metadata', {}).get('finalizers', [])
        patch.setdefault('metadata', {}).setdefault('finalizers', list(finalizers))
        if LEGACY_FINALIZER in patch['metadata']['finalizers']:
            patch['metadata']['finalizers'].remove(LEGACY_FINALIZER)
        if FINALIZER in patch['metadata']['finalizers']:
            patch['metadata']['finalizers'].remove(FINALIZER)
