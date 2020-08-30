"""
All the functions to properly build the object hierarchies.
"""
from typing import Any, Iterable, Iterator, Mapping, MutableMapping, Optional, Union, cast

from kopf.reactor import causation, handling
from kopf.structs import bodies, dicts

K8sObject = MutableMapping[Any, Any]
K8sObjects = Union[K8sObject, Iterable[K8sObject]]


def append_owner_reference(
        objs: K8sObjects,
        owner: Optional[bodies.Body] = None,
) -> None:
    """
    Append an owner reference to the resource(s), if it is not yet there.

    Note: the owned objects are usually not the one being processed,
    so the whole body can be modified, no patches are needed.
    """
    real_owner = _guess_owner(owner)
    owner_ref = bodies.build_owner_reference(real_owner)
    for obj in cast(Iterator[K8sObject], dicts.walk(objs)):
        refs = obj.setdefault('metadata', {}).setdefault('ownerReferences', [])
        matching = [ref for ref in refs if ref.get('uid') == owner_ref['uid']]
        if not matching:
            refs.append(owner_ref)


def remove_owner_reference(
        objs: K8sObjects,
        owner: Optional[bodies.Body] = None,
) -> None:
    """
    Remove an owner reference to the resource(s), if it is there.

    Note: the owned objects are usually not the one being processed,
    so the whole body can be modified, no patches are needed.
    """
    real_owner = _guess_owner(owner)
    owner_ref = bodies.build_owner_reference(real_owner)
    for obj in cast(Iterator[K8sObject], dicts.walk(objs)):
        refs = obj.setdefault('metadata', {}).setdefault('ownerReferences', [])
        matching = [ref for ref in refs if ref.get('uid') == owner_ref['uid']]
        for ref in matching:
            refs.remove(ref)


def label(
        objs: K8sObjects,
        labels: Mapping[str, Union[None, str]],
        *,
        force: bool = False,
        nested: Optional[Iterable[dicts.FieldSpec]] = None,
) -> None:
    """
    Apply the labels to the object(s).
    """
    for obj in cast(Iterator[K8sObject], dicts.walk(objs, nested=nested)):
        obj_labels = obj.setdefault('metadata', {}).setdefault('labels', {})
        for key, val in labels.items():
            if force:
                obj_labels[key] = val
            else:
                obj_labels.setdefault(key, val)


def harmonize_naming(
        objs: K8sObjects,
        name: Optional[str] = None,
        strict: bool = False,
) -> None:
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

    # Try to use the current object being handled if possible.
    if name is None:
        real_owner = _guess_owner(None)
        name = real_owner.get('metadata', {}).get('name', None)

    # Set name/prefix based on the explicitly specified or guessed name.
    for obj in cast(Iterator[K8sObject], dicts.walk(objs)):
        if obj.get('metadata', {}).get('name', None) is None:
            if strict:
                obj.setdefault('metadata', {}).setdefault('name', name)
            else:
                obj.setdefault('metadata', {}).setdefault('generateName', f'{name}-')


def adjust_namespace(
        objs: K8sObjects,
        namespace: Optional[str] = None,
) -> None:
    """
    Adjust the namespace of the objects.

    If the objects already have the namespace set, it will be preserved.

    It is a common practice to keep the children objects in the same
    namespace as their owner, unless explicitly overridden at time of creation.
    """

    # Try to use the current object being handled if possible.
    if namespace is None:
        real_owner = _guess_owner(None)
        namespace = real_owner.get('metadata', {}).get('namespace', None)

    # Set namespace based on the explicitly specified or guessed namespace.
    for obj in cast(Iterator[K8sObject], dicts.walk(objs)):
        obj.setdefault('metadata', {}).setdefault('namespace', namespace)


def adopt(
        objs: K8sObjects,
        owner: Optional[bodies.Body] = None,
        *,
        nested: Optional[Iterable[dicts.FieldSpec]] = None,
) -> None:
    """
    The children should be in the same namespace, named after their parent, and owned by it.
    """
    real_owner = _guess_owner(owner)
    append_owner_reference(objs, owner=real_owner)
    harmonize_naming(objs, name=real_owner.get('metadata', {}).get('name', None))
    adjust_namespace(objs, namespace=real_owner.get('metadata', {}).get('namespace', None))
    label(objs, labels=real_owner.get('metadata', {}).get('labels', {}), nested=nested)


def _guess_owner(
        owner: Optional[bodies.Body],
) -> bodies.Body:
    if owner is not None:
        return owner

    try:
        cause = handling.cause_var.get()
    except LookupError:
        pass
    else:
        if cause is not None and isinstance(cause, causation.ResourceCause):
            return cause.body

    raise LookupError("Owner must be set explicitly, since running outside of a handler.")
