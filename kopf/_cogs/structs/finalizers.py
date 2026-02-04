"""
All the functions to manipulate the object finalization and deletion.

Finalizers are used to block the actual deletion until the finalizers
are removed, meaning that the operator has done all its duties
to "release" the object (e.g. cleanups; delete-handlers in our case).
"""
from kopf._cogs.structs import bodies, patches


def is_deletion_ongoing(
        body: bodies.Body,
) -> bool:
    return body.get('metadata', {}).get('deletionTimestamp', None) is not None


def is_deletion_blocked(
        body: bodies.Body,
        finalizer: str,
) -> bool:
    finalizers = body.get('metadata', {}).get('finalizers', [])
    return finalizer in finalizers


def block_deletion(
        *,
        body: bodies.Body,
        patch: patches.Patch,
        finalizer: str,
) -> None:
    if not is_deletion_blocked(body=body, finalizer=finalizer):
        patch.setdefault('metadata', {}).setdefault('finalizers', [])
        patch['metadata']['finalizers'].append(finalizer)


def allow_deletion(
        *,
        body: bodies.Body,
        patch: patches.Patch,
        finalizer: str,
) -> None:
    if is_deletion_blocked(body=body, finalizer=finalizer):
        patch.setdefault('metadata', {}).setdefault('$deleteFromPrimitiveList/finalizers', [])
        patch['metadata']['$deleteFromPrimitiveList/finalizers'].append(finalizer)
