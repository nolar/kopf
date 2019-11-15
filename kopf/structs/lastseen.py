"""
All the functions to keep track of the last handled state of the object.

The "essence" is a snapshot of meaningful fields, which must be tracked
to identify the actual changes on the object (or absence of such).

Used in the handling routines to check if there were significant changes at all
(i.e. not the internal and system changes, like the uids, links, etc),
and to get the exact per-field diffs for the specific handler functions.

Conceptually similar to how ``kubectl apply`` stores the applied state
on any object, and then uses that for the patch calculation:
https://kubernetes.io/docs/concepts/overview/object-management-kubectl/declarative-config/
"""

import copy
import json
from typing import Optional, Iterable, Tuple, Dict, Any, cast

from kopf.structs import bodies
from kopf.structs import dicts
from kopf.structs import diffs
from kopf.structs import patches

LAST_SEEN_ANNOTATION = 'kopf.zalando.org/last-handled-configuration'
""" The annotation name for the last stored state of the resource. """


def get_essence(
        body: bodies.Body,
        extra_fields: Optional[Iterable[dicts.FieldSpec]] = None,
) -> bodies.BodyEssence:
    """
    Extract only the relevant fields for the state comparisons.

    The framework ignores all the system fields (mostly from metadata)
    and the status senza completely. Except for some well-known and useful
    metadata, such as labels and annotations (except for sure garbage).

    A special set of fields can be provided even if they are supposed
    to be removed. This is used, for example, for handlers which react
    to changes in the specific fields in the status stenza,
    while the rest of the status stenza is removed.
    """

    # Always use a copy, so that future changes do not affect the extracted essence.
    essence = cast(Dict[Any, Any], copy.deepcopy(body))

    # The top-level identifying fields never change, so there is not need to track them.
    if 'apiVersion' in essence:
        del essence['apiVersion']
    if 'kind' in essence:
        del essence['kind']

    # Purge the whole stenzas with system info (extra-fields are restored below).
    if 'metadata' in essence:
        del essence['metadata']
    if 'status' in essence:
        del essence['status']

    # We want some selected metadata to be tracked implicitly.
    dicts.cherrypick(src=body, dst=essence, fields=[
        'metadata.labels',
        'metadata.annotations',  # but not all of them! deleted below.
    ], picker=copy.deepcopy)

    # But we do not want not all of the annotations, only potentially useful.
    annotations = essence.get('metadata', {}).get('annotations', {})
    for annotation in list(annotations):
        if annotation == LAST_SEEN_ANNOTATION:
            del annotations[annotation]
        if annotation == 'kubectl.kubernetes.io/last-applied-configuration':
            del annotations[annotation]

    # Restore all explicitly whitelisted extra-fields from the original body.
    dicts.cherrypick(src=body, dst=essence, fields=extra_fields, picker=copy.deepcopy)

    # Cleanup the parent structs if they have become empty, for consistent essence comparison.
    if 'annotations' in essence.get('metadata', {}) and not essence['metadata']['annotations']:
        del essence['metadata']['annotations']
    if 'metadata' in essence and not essence['metadata']:
        del essence['metadata']
    if 'status' in essence and not essence['status']:
        del essence['status']

    return cast(bodies.BodyEssence, essence)


def has_essence_stored(
        body: bodies.Body,
) -> bool:
    annotations = body.get('metadata', {}).get('annotations', {})
    return LAST_SEEN_ANNOTATION in annotations


def get_essential_diffs(
        body: bodies.Body,
        extra_fields: Optional[Iterable[dicts.FieldSpec]] = None,
) -> Tuple[Optional[bodies.BodyEssence], Optional[bodies.BodyEssence], diffs.Diff]:
    old: Optional[bodies.BodyEssence] = retrieve_essence(body)
    new: Optional[bodies.BodyEssence] = get_essence(body, extra_fields=extra_fields)
    return old, new, diffs.diff(old, new)


def retrieve_essence(
        body: bodies.Body,
) -> Optional[bodies.BodyEssence]:
    if not has_essence_stored(body):
        return None
    essence_str: str = body['metadata']['annotations'][LAST_SEEN_ANNOTATION]
    essence_obj: bodies.BodyEssence = json.loads(essence_str)
    return essence_obj


def refresh_essence(
        *,
        body: bodies.Body,
        patch: patches.Patch,
        extra_fields: Optional[Iterable[dicts.FieldSpec]] = None,
) -> None:
    old_essence = retrieve_essence(body=body)
    new_essence = get_essence(body, extra_fields=extra_fields)
    if new_essence != old_essence:
        annotations = patch.setdefault('metadata', {}).setdefault('annotations', {})
        annotations[LAST_SEEN_ANNOTATION] = json.dumps(new_essence)
