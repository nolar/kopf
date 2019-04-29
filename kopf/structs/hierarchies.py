"""
All the functions to properly build the object hierarchies.
"""
import collections.abc


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
    owner = build_owner_reference(owner)
    for obj in _iter_objects(objs):
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
    owner = build_owner_reference(owner)
    for obj in _iter_objects(objs):
        refs = obj.setdefault('metadata', {}).setdefault('ownerReferences', [])
        matching = [ref for ref in refs if ref['uid'] == owner['uid']]
        for ref in matching:
            refs.remove(ref)


# TODO: make it also recursively if there are any .metadata.labels inside (e.g. job/pod templates).
def label(objs, labels, force=False):
    """
    Apply the labels to the object(s).
    """
    for obj in _iter_objects(objs):
        obj_labels = obj.setdefault('metadata', {}).setdefault('labels', {})
        for key, val in labels.items():
            if force:
                obj_labels[key] = val
            else:
                obj_labels.setdefault(key, val)


def harmonize_naming(objs, name=None, strict=False):
    """
    Adjust the names or prefixes of the objects.

    In strict mode, the provided name is used as is. It can be helpful
    if the object is referred by that name in other objects.

    In non-strict mode (the default), the object uses the provided name
    as a prefix, while the suffix is added by Kubernetes remotely.
    The actual name should be taken from Kubernetes response
    (this is the recommended scenario).

    If the objects already have their own names, auto-naming is not applied,
    and the existing names are used as is.
    """
    for obj in _iter_objects(objs):
        if obj.get('metadata', {}).get('name', None) is None:
            if strict:
                obj.setdefault('metadata', {}).setdefault('name', name)
            else:
                obj.setdefault('metadata', {}).setdefault('generateName', f'{name}-')


def adjust_namespace(objs, namespace=None):
    """
    Adjust the namespace of the objects.

    If the objects already have the namespace set, it will be preserved.

    It is a common practice to keep the children objects in the same
    namespace as their owner, unless explicitly overridden at time of creation.
    """
    for obj in _iter_objects(objs):
        obj.setdefault('metadata', {}).setdefault('namespace', namespace)


def adopt(objs, owner):
    """
    The children should be in the same namespace, named after their parent, and owned by it.
    """
    append_owner_reference(objs, owner=owner)
    harmonize_naming(objs, name=owner.get('metadata', {}).get('name', None))
    adjust_namespace(objs, namespace=owner.get('metadata', {}).get('namespace', None))
    label(objs, labels=owner.get('metadata', {}).get('labels', {}))


def _iter_objects(objs):
    """
    A helper to iterate over one or many objects, as used in these functions.
    """
    if objs is None:
        return
    elif isinstance(objs, collections.abc.Mapping):  # also iterable, intercept it.
        yield objs
    elif isinstance(objs, collections.abc.Iterable):
        yield from objs
    else:
        yield objs
