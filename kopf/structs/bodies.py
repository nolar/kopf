"""
All the structures coming from/to the Kubernetes API.
"""


def build_object_reference(body):
    """
    Construct an object reference for the events.

    Keep in mind that some fields can be absent: e.g. ``namespace``
    for cluster resources, or e.g. ``apiVersion`` for ``kind: Node``, etc.
    """
    ref = dict(
        apiVersion=body.get('apiVersion'),
        kind=body.get('kind'),
        name=body.get('metadata', {}).get('name'),
        uid=body.get('metadata', {}).get('uid'),
        namespace=body.get('metadata', {}).get('namespace'),
    )
    return {key: val for key, val in ref.items() if val}


def build_owner_reference(body):
    """
    Construct an owner reference object for the parent-children relationships.

    The structure needed to link the children objects to the current object as a parent.
    See https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/

    Keep in mind that some fields can be absent: e.g. ``namespace``
    for cluster resources, or e.g. ``apiVersion`` for ``kind: Node``, etc.
    """
    ref = dict(
        controller=True,
        blockOwnerDeletion=True,
        apiVersion=body.get('apiVersion'),
        kind=body.get('kind'),
        name=body.get('metadata', {}).get('name'),
        uid=body.get('metadata', {}).get('uid'),
    )
    return {key: val for key, val in ref.items() if val}
