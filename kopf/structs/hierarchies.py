"""
All the functions to properly build the object hierarchies.
"""


def build_object_reference(body):
    """
    Construct an object reference for the events.
    """
    return dict(
        apiVersion=body['apiVersion'],
        kind=body['kind'],
        name=body['metadata']['name'],
        uid=body['metadata']['uid'],
        namespace=body['metadata']['namespace'],
    )


def build_owner_reference(body):
    """
    Construct an owner reference object for the parent-children relationships.

    The structure needed to link the children objects to the current object as a parent.
    See https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/
    """
    return dict(
        controller=True,
        blockOwnerDeletion=True,
        apiVersion=body['apiVersion'],
        kind=body['kind'],
        name=body['metadata']['name'],
        uid=body['metadata']['uid'],
    )


def append_owner_reference(objs, owner):
    """
    Append an owner reference to the resource(s), if it is not yet there.

    Note: the owned objects are usually not the one being processed,
    so the whole body can be modified, no patches are needed.
    """
    if not isinstance(objs, (list, tuple)):
        objs = [objs]

    owner = build_owner_reference(owner)
    for obj in objs:
        refs = obj.setdefault('metadata', {}).setdefault('ownerReferences', [])
        matching = [ref for ref in refs if ref['uid'] == owner['uid']]
        if not matching:
            refs.append(owner)


def remove_owner_reference(objs, owner):
    """
    Remove an owner reference to the resource(s), if it is there.

    Note: the owned objects are usually not the one being processed,
    so the whole body can be modified, no patches are needed.
    """
    if not isinstance(objs, (list, tuple)):
        objs = [objs]

    owner = build_owner_reference(owner)
    for obj in objs:
        refs = obj.setdefault('metadata', {}).setdefault('ownerReferences', [])
        matching = [ref for ref in refs if ref['uid'] == owner['uid']]
        for ref in matching:
            refs.remove(ref)


# TODO: make it also recursively if there are any .metadata.labels inside (e.g. job/pod templates).
def label(objs, labels, force=False):
    """
    Apply the labels to the object(s).
    """
    if not isinstance(objs, (list, tuple)):
        objs = [objs]

    for obj in objs:
        obj_labels = obj.setdefault('metadata', {}).setdefault('labels', {})
        for key, val in labels.items():
            if force:
                obj_labels[key] = val
            else:
                obj_labels.setdefault(key, val)


def adopt(objs, owner):
    """
    The children should be in the same namespace, named after their parent, and owned by it.
    """
    if not isinstance(objs, (list, tuple)):
        objs = [objs]

    # Mark the children as owned by the parent.
    append_owner_reference(objs, owner=owner)

    # The children objects are usually in the same namespace as the parent, unless explicitly overridden.
    ns = owner.get('metadata', {}).get('namespace', None)
    if ns is not None:
        for obj in objs:
            obj.setdefault('metadata', {}).setdefault('namespace', ns)

    # Name the children prefixed with their parent's name, unless they already have a name or a prefix.
    # "GenerateName" is the Kubernetes feature, we do not implement it ourselves.
    name = owner.get('metadata', {}).get('name', None)
    if name is not None:
        for obj in objs:
            if obj.get('metadata', {}).get('name', None) is None:
                obj.setdefault('metadata', {}).setdefault('generateName', f'{name}-')

    # The children also bear the labels of the parent object, for easier selection.
    label(objs, labels=owner.get('metadata', {}).get('labels', {}))
