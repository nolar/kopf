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
import json
from collections.abc import Collection
from typing import Literal, TypedDict

import aiohttp

# How many characters of the non-JSON (textual) API errors to log.
# 256 is an arbitrary size based on the gut feeling of what is good for logs & enough for debugging.
# Plus a little hope that no sensitive information usually goes in the leading 256 characters.
# It is not a guarantee, but reduces the probabilities of undesired consequences.
TEXT_ERROR_MAX_SIZE = 256


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
            payload: RawStatus | str | None = None,
            *,
            status: int,
            headers: dict[str, str],
    ) -> None:
        if isinstance(payload, str):
            super().__init__(payload)
        elif payload:
            super().__init__(payload.get('message'), payload)
        else:
            super().__init__()
        self._status = status
        self._headers = headers
        self._payload = payload

    def __repr__(self) -> str:
        subreprs = [repr(arg) for arg in self.args]
        subreprs.append(f'status={self._status!r}')
        return f"{self.__class__.__name__}({', '.join(subreprs)})"

    @property
    def status(self) -> int:
        return self._status

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    @property
    def code(self) -> int | None:
        return self._payload.get('code') if isinstance(self._payload, dict) else None

    @property
    def message(self) -> str | None:
        return self._payload.get('message') if isinstance(self._payload, dict) else None

    @property
    def details(self) -> RawStatusDetails | None:
        return self._payload.get('details') if isinstance(self._payload, dict) else None


class APIClientError(APIError):  # all 4xx
    pass


class APIServerError(APIError):  # all 5xx
    pass


class APIUnauthorizedError(APIClientError):
    pass


class APIForbiddenError(APIClientError):
    pass


class APINotFoundError(APIClientError):
    pass


class APIConflictError(APIClientError):
    pass


class APITooManyRequestsError(APIClientError):
    pass


class APISessionClosed(Exception):
    """
    A helper to escalate from inside the requests to cause re-authentication.

    This happens when credentials expire while multiple concurrent requests
    are ongoing (including their retries, mostly their back-off timeouts):
    one random request will raise HTTP 401 and cause the re-authentication,
    while others will retry their requests with the old session (now closed!)
    and get a generic RuntimeError from aiohttp, thus failing their whole task.
    """
    pass


async def check_response(
        response: aiohttp.ClientResponse,
) -> None:
    """
    Check for specialised K8s errors, and raise with extended information.
    """
    if response.status >= 400:

        # Read the response's body before it is closed by raise_for_status().
        payload: RawStatus | str | None
        try:
            payload = await response.json()
        except (json.JSONDecodeError, aiohttp.ContentTypeError, aiohttp.ClientConnectionError):
            payload = await response.text()
            payload = payload.strip() or None

        # Better be safe: who knows which sensitive information can be dumped unless kind==Status.
        if isinstance(payload, dict) and payload.get('kind') != 'Status':
            payload = None

        # Better be safe: if a data blob (not an error) is dumped, protect the logs from overflows.
        if isinstance(payload, str) and len(payload) >= TEXT_ERROR_MAX_SIZE:
            payload = payload[:TEXT_ERROR_MAX_SIZE-3] + '...'

        cls = (
            APIUnauthorizedError if response.status == 401 else
            APIForbiddenError if response.status == 403 else
            APINotFoundError if response.status == 404 else
            APIConflictError if response.status == 409 else
            APITooManyRequestsError if response.status == 429 else
            APIClientError if 400 <= response.status < 500 else
            APIServerError if 500 <= response.status < 600 else
            APIError
        )

        # Raise the framework-specific error while keeping the original error in scope.
        # This call also closes the response's body, so it cannot be read afterwards.
        try:
            response.raise_for_status()
        except aiohttp.ClientResponseError as e:
            raise cls(payload, status=response.status, headers=dict(response.headers)) from e
