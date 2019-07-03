"""
All the functions to keep track of the last seen state.

The "state" is a snapshot of meaningful fields, which must be tracked
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

from kopf.structs import dicts
from kopf.structs import diffs

LAST_SEEN_ANNOTATION = 'kopf.zalando.org/last-handled-configuration'
""" The annotation name for the last stored state of the resource. """


def get_state(body, extra_fields=None):
    """
    Extract only the relevant fields for the state comparisons.

    A special set of fields can be provided even if they are supposed
    to be removed. This is used, for example, for handlers which react
    to changes in the specific fields in the status stenza,
    while the rest of the status stenza is removed.
    """

    # Always use a copy, so that future changes do not affect the extracted state.
    orig = copy.deepcopy(body)
    body = copy.deepcopy(body)

    # Purge the whole stenzas with system info (extra-fields are restored below).
    if 'status' in body:
        del body['status']

    # Remove the system fields, keeping the potentially useful, user-oriented fields/data.
    if LAST_SEEN_ANNOTATION in body.get('metadata', {}).get('annotations', {}):
        del body['metadata']['annotations'][LAST_SEEN_ANNOTATION]
    if 'kubectl.kubernetes.io/last-applied-configuration' in body.get('metadata', {}).get('annotations', {}):
        del body['metadata']['annotations']['kubectl.kubernetes.io/last-applied-configuration']
    if 'finalizers' in body.get('metadata', {}):
        del body['metadata']['finalizers']
    if 'deletionTimestamp' in body.get('metadata', {}):
        del body['metadata']['deletionTimestamp']
    if 'creationTimestamp' in body.get('metadata', {}):
        del body['metadata']['creationTimestamp']
    if 'selfLink' in body.get('metadata', {}):
        del body['metadata']['selfLink']
    if 'uid' in body.get('metadata', {}):
        del body['metadata']['uid']
    if 'resourceVersion' in body.get('metadata', {}):
        del body['metadata']['resourceVersion']
    if 'generation' in body.get('metadata', {}):
        del body['metadata']['generation']

    # Restore all explicitly whitelisted extra-fields from the original body.
    dicts.cherrypick(src=orig, dst=body, fields=extra_fields)

    # Cleanup the parent structs if they have become empty, for consistent state comparison.
    if 'annotations' in body.get('metadata', {}) and not body['metadata']['annotations']:
        del body['metadata']['annotations']
    if 'metadata' in body and not body['metadata']:
        del body['metadata']
    if 'status' in body and not body['status']:
        del body['status']
    return body


def has_state(body):
    annotations = body.get('metadata', {}).get('annotations', {})
    return LAST_SEEN_ANNOTATION in annotations


def get_state_diffs(body, extra_fields=None):
    old = retreive_state(body)
    new = get_state(body, extra_fields=extra_fields)
    return old, new, diffs.diff(old, new)


def retreive_state(body):
    state_str = body.get('metadata', {}).get('annotations', {}).get(LAST_SEEN_ANNOTATION, None)
    state_obj = json.loads(state_str) if state_str is not None else None
    return state_obj


def refresh_state(*, body, patch, extra_fields=None):
    state = get_state(body, extra_fields=extra_fields)
    patch.setdefault('metadata', {}).setdefault('annotations', {})[LAST_SEEN_ANNOTATION] = json.dumps(state)
