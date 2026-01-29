"""
Admission reviews: requests & responses, also the webhook server protocols.
"""
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Any, Literal, Protocol, TypedDict

from kopf._cogs.structs import bodies

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
    groups: list[str]


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
    subResource: str | None
    requestKind: RequestKind
    requestResource: RequestResource
    requestSubResource: str | None
    userInfo: UserInfo
    name: str
    namespace: str | None
    operation: Operation
    options: CreateOptions | UpdateOptions | DeleteOptions | None
    dryRun: bool
    object: bodies.RawBody
    oldObject: bodies.RawBody | None


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
    warnings: list[str] | None
    status: ResponseStatus | None
    patch: str | None
    patchType: Literal["JSONPatch"] | None


class Response(TypedDict):
    apiVersion: Literal["admission.k8s.io/v1", "admission.k8s.io/v1beta1"]
    kind: Literal["AdmissionReview"]
    response: ResponsePayload


class WebhookClientConfigService(TypedDict, total=False):
    namespace: str | None
    name: str | None
    path: str | None
    port: int | None


class WebhookClientConfig(TypedDict, total=False):
    """
    A config of clients (apiservers) to access the webhooks' server (operators).

    This dictionary is put into managed webhook configurations "as is".
    The fields & type annotations are only for hinting.

    Kopf additionally modifies the url and the service's path to inject
    handler ids as the last path component. This must be taken into account
    by custom webhook servers.
    """
    caBundle: str | None  # if absent, the default apiservers' trust chain is used.
    url: str | None
    service: WebhookClientConfigService | None


class WebhookFn(Protocol):
    """
    A framework-provided function to call when an admission request is received.

    The framework provides the actual function. Custom webhook servers must
    accept the function, invoke it accordingly on admission requests, wait
    for the admission response, serialise it and send it back. They do not
    implement this function. This protocol only declares the exact signature.
    """
    def __call__(
            self,
            request: Request,
            *,
            webhook: str | None = None,
            headers: Mapping[str, str] | None = None,
            sslpeer: Mapping[str, Any] | None = None,
    ) -> Awaitable[Response]:
        ...


# A server (either a coroutine or a callable object).
WebhookServerProtocol = Callable[[WebhookFn], AsyncIterator[WebhookClientConfig]]
