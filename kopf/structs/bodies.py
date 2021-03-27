"""
All the structures coming from/to the Kubernetes API.

The usage of these classes is spread over the codebase, so they are extracted
into a separate module of such type definitions.

For strict type-checking, they are detailed to the per-field level
(e.g. `TypedDict` instead of just ``Mapping[Any, Any]``) --
as used by the framework. The operators can use arbitrary fields at runtime,
which are not declared in the type definitions at type-checking time.

In case the operators are also type-checked, type casting can be used
(without `cast`, this code fails at type-checking, though works at runtime)::

    from typing import cast
    import kopf

    class MyMeta(kopf.Meta):
        unknownField: str

    @kopf.on.create('kopfexamples')
    def create_fn(*args, meta: kopf.Meta, **kwargs):
        meta = cast(MyMeta, meta)
        print(meta['unknownField'])

.. note::

    There is a strict separation of objects coming from/to the Kubernetes API
    and from (but not to) the users:

    The Kubernetes-originated objects are dicts or dict-like custom classes.
    The framework internally expect them to be such. Arbitrary 3rd-party
    classes are not supported and are not delivered to the handlers.

    The user-originated objects can be either one of the Kubernetes-originated
    framework-supported types (dicts/dict-like), or a 3rd-party class,
    such as from ``pykube-ng``, ``kubernetes`` client, etc -- as long as it is
    supported by the framework's object-processing functions.

    In the future, extra classes can be added for the user-originated objects
    and object-processing functions. The internal dicts will remain the same.
"""

from typing import Any, List, Mapping, Optional, Union, cast

from typing_extensions import Literal, TypedDict

from kopf.structs import dicts, references

# Make sure every kwarg has a corresponding same-named type in the root package.
Labels = Mapping[str, str]
Annotations = Mapping[str, str]

#
# Everything marked "raw" is a plain unwrapped unprocessed data as JSON-decoded
# from Kubernetes API, usually as retrieved in watching or fetching API calls.
# "Input" is a parsed JSON as is, while "event" is an "input" without "errors".
# All non-used payload falls into `Any`, and is not type-checked.
#

# ``None`` is used for the listing, when the pseudo-watch-stream is simulated.
RawInputType = Literal[None, 'ADDED', 'MODIFIED', 'DELETED', 'ERROR']
RawEventType = Literal[None, 'ADDED', 'MODIFIED', 'DELETED']


class RawMeta(TypedDict, total=False):
    uid: str
    name: str
    namespace: str
    labels: Labels
    annotations: Annotations
    finalizers: List[str]
    resourceVersion: str
    deletionTimestamp: str
    creationTimestamp: str
    selfLink: str


class RawBody(TypedDict, total=False):
    apiVersion: str
    kind: str
    metadata: RawMeta
    spec: Mapping[str, Any]
    status: Mapping[str, Any]


# A special payload for type==ERROR (this is not a connection or client error).
class RawError(TypedDict, total=False):
    apiVersion: str     # usually: Literal['v1']
    kind: str           # usually: Literal['Status']
    metadata: Mapping[Any, Any]
    code: int
    reason: str
    status: str
    message: str


# As received from the stream before processing the errors and special cases.
class RawInput(TypedDict, total=True):
    type: RawInputType
    object: Union[RawBody, RawError]


# As passed to the framework after processing the errors and special cases.
class RawEvent(TypedDict, total=True):
    type: RawEventType
    object: RawBody


#
# Body/Meta essences only contain the fields relevant for object diff tracking.
# They are presented to the user as part of the diff's `old`/`new` fields & kwargs.
# Added for stricter type checking, to differentiate from the actual Body/Meta.
#


class MetaEssence(TypedDict, total=False):
    labels: Labels
    annotations: Annotations


class BodyEssence(TypedDict, total=False):
    metadata: MetaEssence
    spec: Mapping[str, Any]
    status: Mapping[str, Any]


#
# Enhanced dict-wrappers for easier typed access to well-known typed fields,
# with live view of updates and changes in the root body (for daemon's)
# Despite they are just MappingViews with no extensions, they are separated
# for stricter typing of arguments.
#


class Meta(dicts.MappingView[str, Any]):

    def __init__(self, __src: "Body") -> None:
        super().__init__(__src, 'metadata')
        self._labels: dicts.MappingView[str, str] = dicts.MappingView(self, 'labels')
        self._annotations: dicts.MappingView[str, str] = dicts.MappingView(self, 'annotations')

    @property
    def labels(self) -> Labels:
        return self._labels

    @property
    def annotations(self) -> Annotations:
        return self._annotations

    @property
    def uid(self) -> Optional[str]:
        return cast(Optional[str], self.get('uid'))

    @property
    def name(self) -> Optional[str]:
        return cast(Optional[str], self.get('name'))

    @property
    def namespace(self) -> references.Namespace:
        return cast(references.Namespace, self.get('namespace'))

    @property
    def creation_timestamp(self) -> Optional[str]:
        return cast(Optional[str], self.get('creationTimestamp'))

    @property
    def deletion_timestamp(self) -> Optional[str]:
        return cast(Optional[str], self.get('deletionTimestamp'))


class Spec(dicts.MappingView[str, Any]):
    def __init__(self, __src: "Body") -> None:
        super().__init__(__src, 'spec')


class Status(dicts.MappingView[str, Any]):
    def __init__(self, __src: "Body") -> None:
        super().__init__(__src, 'status')


class Body(dicts.ReplaceableMappingView[str, Any]):

    def __init__(self, __src: Mapping[str, Any]) -> None:
        super().__init__(__src)
        self._meta = Meta(self)
        self._spec = Spec(self)
        self._status = Status(self)

    @property
    def metadata(self) -> Meta:
        return self._meta

    @property
    def meta(self) -> Meta:
        return self._meta

    @property
    def spec(self) -> Spec:
        return self._spec

    @property
    def status(self) -> Status:
        return self._status


#
# Other API types, which are not body parts.
#

class ObjectReference(TypedDict, total=False):
    apiVersion: str
    kind: str
    namespace: Optional[str]
    name: str
    uid: str


class OwnerReference(TypedDict, total=False):
    controller: bool
    blockOwnerDeletion: bool
    apiVersion: str
    kind: str
    name: str
    uid: str


def build_object_reference(
        body: Body,
) -> ObjectReference:
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
    return cast(ObjectReference, {key: val for key, val in ref.items() if val})


def build_owner_reference(
        body: Body,
) -> OwnerReference:
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
    return cast(OwnerReference, {key: val for key, val in ref.items() if val})
