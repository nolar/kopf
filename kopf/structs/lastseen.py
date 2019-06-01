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

from kopf.structs import diffs

LAST_SEEN_ANNOTATION = 'kopf.zalando.org/last-handled-configuration'
""" The annotation name for the last stored state of the resource. """


def get_state(body):
    """
    Extract only the relevant fields for the state comparisons.
    """

    # Always use a copy, so that future changes do not affect the extracted state.
    body = copy.deepcopy(body)

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
    if 'kopf' in body.get('status', {}):
        del body['status']['kopf']

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


def is_state_changed(body):
    # TODO: make it more efficient, so that the dicts are not rebuilt locally every time.
    old = retreive_state(body)
    new = get_state(body)
    return old != new


def get_state_diffs(body):
    old = retreive_state(body)
    new = get_state(body)
    return old, new, diffs.diff(old, new)


def retreive_state(body):
    state_str = body.get('metadata', {}).get('annotations', {}).get(LAST_SEEN_ANNOTATION, None)
    state_obj = json.loads(state_str) if state_str is not None else None
    return state_obj


def refresh_state(*, body, patch):
    state = get_state(body)
    patch.setdefault('metadata', {}).setdefault('annotations', {})[LAST_SEEN_ANNOTATION] = json.dumps(state)
