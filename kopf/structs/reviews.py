"""
Admission reviews: requests & responses, also the webhook server protocols.
"""
from typing import Any, List, Mapping, Optional, Union

from typing_extensions import Literal, TypedDict

from kopf.structs import bodies

Headers = Mapping[str, str]
SSLPeer = Mapping[str, Any]

Operation = Literal['CREATE', 'UPDATE', 'DELETE', 'CONNECT']


class RequestKind(TypedDict):
    group: str
    version: str
    kind: str


class RequestResource(TypedDict):
    group: str
    version: str
    resource: str


class UserInfo(TypedDict):
    username: str
    uid: str
    groups: List[str]


class CreateOptions(TypedDict, total=False):
    apiVersion: Literal["meta.k8s.io/v1"]
    kind: Literal["CreateOptions"]


class UpdateOptions(TypedDict, total=False):
    apiVersion: Literal["meta.k8s.io/v1"]
    kind: Literal["UpdateOptions"]


class DeleteOptions(TypedDict, total=False):
    apiVersion: Literal["meta.k8s.io/v1"]
    kind: Literal["DeleteOptions"]


class RequestPayload(TypedDict):
    uid: str
    kind: RequestKind
    resource: RequestResource
    requestKind: RequestKind
    requestResource: RequestResource
    userInfo: UserInfo
    name: str
    namespace: Optional[str]
    operation: Operation
    options: Union[None, CreateOptions, UpdateOptions, DeleteOptions]
    dryRun: bool
    object: bodies.RawBody
    oldObject: Optional[bodies.RawBody]


class Request(TypedDict):
    apiVersion: Literal["admission.k8s.io/v1", "admission.k8s.io/v1beta1"]
    kind: Literal["AdmissionReview"]
    request: RequestPayload


class ResponseStatus(TypedDict, total=False):
    code: int
    message: str


class ResponsePayload(TypedDict, total=False):
    uid: str
    allowed: bool
    warnings: Optional[List[str]]
    status: Optional[ResponseStatus]
    patch: Optional[str]
    patchType: Optional[Literal["JSONPatch"]]


class Response(TypedDict):
    apiVersion: Literal["admission.k8s.io/v1", "admission.k8s.io/v1beta1"]
    kind: Literal["AdmissionReview"]
    response: ResponsePayload
