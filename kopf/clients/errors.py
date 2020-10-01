import collections.abc
import json

import aiohttp


class APIClientResponseError(aiohttp.ClientResponseError):
    """
    Same as :class:`aiohttp.ClientResponseError`, but with information from K8s.
    """


async def check_response(
        response: aiohttp.ClientResponse,
) -> None:
    """
    Check for specialised K8s errors, and raise with extended information.

    Built-in aiohttp's errors only provide the rudimentary titles of the status
    codes, but not the explanation why that error happened. K8s API provides
    this information in the bodies of non-2xx responses. That information can
    replace aiohttp's error messages to be more helpful.

    However, the same error classes are used for now, to meet the expectations
    if some routines in their ``except:`` clauses analysing for specific HTTP
    statuses: e.g. 401 for re-login activities, 404 for patching/deletion, etc.
    """
    if response.status >= 400:
        try:
            payload = await response.json()
        except (json.JSONDecodeError, aiohttp.ContentTypeError, aiohttp.ClientConnectionError):
            payload = None

        # Better be safe: who knows which sensitive information can be dumped unless kind==Status.
        if not isinstance(payload, collections.abc.Mapping) or payload.get('kind') != 'Status':
            payload = None

        # If no information can be retrieved, fall back to the original error.
        if payload is None:
            response.raise_for_status()
        else:
            details = payload.get('details')
            message = payload.get('message') or f"{details}"
            raise APIClientResponseError(
                response.request_info,
                response.history,
                status=response.status,
                headers=response.headers,
                message=message)
