"""
Admission reviews: requests & responses, also the webhook server protocols.
"""
from typing import Any, AsyncIterator, Awaitable, Callable, List, Mapping, Optional, Union

from typing_extensions import Literal, Protocol, TypedDict

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


class WebhookClientConfigService(TypedDict, total=False):
    namespace: Optional[str]
    name: Optional[str]
    path: Optional[str]
    port: Optional[int]


class WebhookClientConfig(TypedDict, total=False):
    """
    A config of clients (apiservers) to access the webhooks' server (operators).

    This dictionary is put into managed webhook configurations "as is".
    The fields & type annotations are only for hinting.

    Kopf additionally modifies the url and the service's path to inject
    handler ids as the last path component. This must be taken into account
    by custom webhook servers.
    """
    caBundle: Optional[str]  # if absent, the default apiservers' trust chain is used.
    url: Optional[str]
    service: Optional[WebhookClientConfigService]


class WebhookFn(Protocol):
    """
    A framework-provided function to call when a admission request is received.

    The framework provides the actual function. Custom webhook servers must
    accept the function, invoke it accordingly on admission requests, wait
    for the admission response, serialise it and send it back. They do not
    implement this function. This protocol only declares the exact signature.
    """
    def __call__(
            self,
            request: Request,
            *,
            webhook: Optional[str] = None,
            headers: Optional[Mapping[str, str]] = None,
            sslpeer: Optional[Mapping[str, Any]] = None,
    ) -> Awaitable[Response]: ...


# A server (either a coroutine or a callable object).
WebhookServerProtocol = Callable[[WebhookFn], AsyncIterator[WebhookClientConfig]]
