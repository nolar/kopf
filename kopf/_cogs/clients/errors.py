"""
K8s API errors.

The underlying client library (now, ``aiohttp``) can be replaced in the future.
We cannot rely on embedding its exceptions all over the code in the framework.
Hence, we have our own hierarchy of exceptions for K8s API errors.

Low-level errors, such as the network connectivity issues, SSL/HTTPS issues,
etc, are escalated from the client library as is, since they are related not
to the domain of K8s API, but rather to the networking and encryption.

The original errors of the client library are chained as the causes of our own
specialised errors -- for better explainability of errors in the stack traces.

Some selected reasons of K8s API errors are made into their own classes,
so that they could be intercepted and handled in other places of the framework.
All other reasons are raised as the base error class and are indistinguishable
from each other (except via the exception's fields).

Unlike the underlying client library's errors, the K8s API errors contain more
information about the reasons -- as provided by K8s API in its response bodies,
not guessed only by HTTP statuses alone.

These errors are not exposed to the users, and the users cannot catch them
with ``except:`` clauses. The users can only see these errors in the logs
as the reasons of failures. However, the errors are exposed to other packages.
"""
import collections.abc
import json
from typing import Any, Collection, Optional

import aiohttp
from typing_extensions import Literal, TypedDict


class RawStatusCause(TypedDict):
    field: str
    reason: str
    message: str


class RawStatusDetails(TypedDict):
    name: str
    uid: str
    retryAfterSeconds: int
    kind: str
    group: str
    causes: Collection[RawStatusCause]


# https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#status-v1-meta
class RawStatus(TypedDict):
    apiVersion: str
    kind: Literal["Status"]
    code: int
    status: Literal["Success", "Failure"]
    reason: str
    message: str
    details: RawStatusDetails


class APIError(Exception):

    def __init__(
            self,
            payload: Optional[RawStatus],
            *,
            status: int,
    ) -> None:
        message = payload.get('message') if payload else None
        super().__init__(message, payload)
        self._status = status
        self._payload = payload

    @property
    def status(self) -> int:
        return self._status

    @property
    def code(self) -> Optional[int]:
        return self._payload.get('code') if self._payload else None

    @property
    def message(self) -> Optional[str]:
        return self._payload.get('message') if self._payload else None

    @property
    def details(self) -> Optional[RawStatusDetails]:
        return self._payload.get('details') if self._payload else None


class APIUnauthorizedError(APIError):
    pass


class APIForbiddenError(APIError):
    pass


class APINotFoundError(APIError):
    pass


class APIConflictError(APIError):
    pass


async def check_response(
        response: aiohttp.ClientResponse,
) -> None:
    """
    Check for specialised K8s errors, and raise with extended information.
    """
    if response.status >= 400:

        # Read the response's body before it is closed by raise_for_status().
        payload: Optional[RawStatus]
        try:
            payload = await response.json()
        except (json.JSONDecodeError, aiohttp.ContentTypeError, aiohttp.ClientConnectionError):
            payload = None

        # Better be safe: who knows which sensitive information can be dumped unless kind==Status.
        if not isinstance(payload, collections.abc.Mapping) or payload.get('kind') != 'Status':
            payload = None

        cls = (
            APIUnauthorizedError if response.status == 401 else
            APIForbiddenError if response.status == 403 else
            APINotFoundError if response.status == 404 else
            APIConflictError if response.status == 409 else
            APIError
        )

        # Raise the framework-specific error while keeping the original error in scope.
        # This call also closes the response's body, so it cannot be read afterwards.
        try:
            response.raise_for_status()
        except aiohttp.ClientResponseError as e:
            raise cls(payload, status=response.status) from e


async def parse_response(
        response: aiohttp.ClientResponse,
) -> Any:
    """
    Check the response for errors, and either raise or returned the parsed data.
    """
    await check_response(response)
    payload = await response.json()
    return payload
