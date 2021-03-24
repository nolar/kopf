import asyncio
import base64
import json
from typing import Any, Collection, List, Mapping, Optional

from kopf.engines import loggers
from kopf.reactor import causation, handling, lifecycles, registries
from kopf.storage import states
from kopf.structs import bodies, configuration, containers, ephemera, \
                         handlers, patches, primitives, references, reviews


class AdmissionError(handling.PermanentError):
    """
    Raised by admission handlers when an API operation under check is bad.

    An admission error behaves the same as `kopf.PermanentError`, but provides
    admission-specific payload for the response: a message & a numeric code.

    This error type is preferred when selecting only one error to report back
    to apiservers as the admission review result -- in case multiple handlers
    are called in one admission request, i.e. when the webhook endpoints
    are not mapped to the handler ids (e.g. when configured manually).
    """
    def __init__(
            self,
            message: Optional[str] = '',
            code: Optional[int] = 500,
    ) -> None:
        super().__init__(message)
        self.code = code


class WebhookError(Exception):
    """
    Raised when a webhook request is bad, not an API operation under check.
    """


class MissingDataError(WebhookError):
    """ An admission is requested but some expected data are missing. """


class UnknownResourceError(WebhookError):
    """ An admission is made for a resource that the operator does not have. """


class AmbiguousResourceError(WebhookError):
    """ An admission is made for one resource, but we (somehow) found a few. """


async def serve_admission_request(
        # Required for all webhook servers, meaningless without it:
        request: reviews.Request,
        *,
        # Optional for webhook servers that can recognise this information:
        headers: Optional[Mapping[str, str]] = None,
        sslpeer: Optional[Mapping[str, Any]] = None,
        webhook: Optional[handlers.HandlerId] = None,
        reason: Optional[handlers.WebhookType] = None,  # TODO: undocumented: requires typing clarity!
        # Injected by partial() from spawn_tasks():
        settings: configuration.OperatorSettings,
        memories: containers.ResourceMemories,
        memobase: ephemera.AnyMemo,
        registry: registries.OperatorRegistry,
        insights: references.Insights,
        indices: ephemera.Indices,
) -> reviews.Response:
    """
    The actual and the only implementation of the `WebhookFn` protocol.

    This function is passed to all webhook servers/tunnels to be called
    whenever a new admission request is received.

    Some parameters are provided by the framework itself via partial binding,
    so that the resulting function matches the `WebhookFn` protocol. Other
    parameters are passed by the webhook servers when they call the function.
    """

    # Reconstruct the cause specially for web handlers.
    resource = find_resource(request=request, insights=insights)
    operation = request.get('request', {}).get('operation')
    userinfo = request.get('request', {}).get('userInfo')
    new_body = request.get('request', {}).get('object')
    old_body = request.get('request', {}).get('oldObject')
    raw_body = new_body if new_body is not None else old_body
    if userinfo is None:
        raise MissingDataError("User info is missing from the admission request.")
    if raw_body is None:
        raise MissingDataError("Either old or new object is missing from the admission request.")

    memo = await memories.recall(raw_body, memo=memobase, ephemeral=operation=='CREATE')
    body = bodies.Body(raw_body)
    patch = patches.Patch()
    warnings: List[str] = []
    cause = causation.ResourceWebhookCause(
        resource=resource,
        indices=indices,
        logger=loggers.LocalObjectLogger(body=body, settings=settings),
        patch=patch,
        memo=memo,
        body=body,
        userinfo=userinfo,
        warnings=warnings,
        operation=operation,
        dryrun=bool(request.get('request', {}).get('dryRun')),
        sslpeer=sslpeer if sslpeer is not None else {},  # ensure a mapping even if not provided.
        headers=headers if headers is not None else {},  # ensure a mapping even if not provided.
        webhook=webhook,
        reason=reason,
    )

    # Retrieve the handlers to be executed; maybe only one if the webhook server provides a hint.
    handlers_ = registry._resource_webhooks.get_handlers(cause)
    state = states.State.from_scratch().with_handlers(handlers_)
    outcomes = await handling.execute_handlers_once(
        lifecycle=lifecycles.all_at_once,
        settings=settings,
        handlers=handlers_,
        cause=cause,
        state=state,
        default_errors=handlers.ErrorsMode.PERMANENT,
    )

    # Construct the response as per Kubernetes's conventions and expectations.
    response = build_response(
        request=request,
        outcomes=outcomes,
        warnings=warnings,
        jsonpatch=patch.as_json_patch(),
    )
    return response


def find_resource(
        *,
        request: reviews.Request,
        insights: references.Insights,
) -> references.Resource:
    """
    Identify the requested resource by its meta-information (as discovered).
    """
    # NB: Absent keys in the request are not acceptable, they must be provided.
    request_payload: reviews.RequestPayload = request['request']
    request_resource: reviews.RequestResource = request_payload['resource']
    group = request_resource['group']
    version = request_resource['version']
    plural = request_resource['resource']
    selector = references.Selector(group=group, version=version, plural=plural)
    resources = selector.select(insights.resources)
    if not resources:
        raise UnknownResourceError(f"The specified resource has no handlers: {request_resource}")
    elif len(resources) > 1:
        raise AmbiguousResourceError(f"The specified resource is ambiguous: {request_resource}")
    else:
        return list(resources)[0]


def build_response(
        *,
        request: reviews.Request,
        outcomes: Mapping[handlers.HandlerId, states.HandlerOutcome],
        warnings: Collection[str],
        jsonpatch: patches.JSONPatch,
) -> reviews.Response:
    """
    Construct the admission review response to a review request.
    """
    allowed = all(outcome.exception is None for id, outcome in outcomes.items())
    response = reviews.Response(
        apiVersion=request.get('apiVersion', 'admission.k8s.io/v1'),
        kind=request.get('kind', 'AdmissionReview'),
        response=reviews.ResponsePayload(
            uid=request.get('request', {}).get('uid', ''),
            allowed=allowed))
    if warnings:
        response['response']['warnings'] = [str(warning) for warning in warnings]
    if jsonpatch:
        encoded_patch: str = base64.b64encode(json.dumps(jsonpatch).encode('utf-8')).decode('ascii')
        response['response']['patch'] = encoded_patch
        response['response']['patchType'] = 'JSONPatch'

    # Prefer specialised admission errors to all other errors, Kopf's own errors to arbitrary ones.
    errors = [outcome.exception for outcome in outcomes.values() if outcome.exception is not None]
    errors.sort(key=lambda error: (
        0 if isinstance(error, AdmissionError) else
        1 if isinstance(error, handling.PermanentError) else
        2 if isinstance(error, handling.TemporaryError) else
        9
    ))
    if errors:
        response['response']['status'] = reviews.ResponseStatus(
            message=str(errors[0]) or repr(errors[0]),
            code=(errors[0].code if isinstance(errors[0], AdmissionError) else None) or 500,
        )
    return response


async def admission_webhook_server(
        *,
        settings: configuration.OperatorSettings,
        registry: registries.OperatorRegistry,
        insights: references.Insights,
        webhookfn: reviews.WebhookFn,
        container: primitives.Container[reviews.WebhookClientConfig],
) -> None:

    # Verify that the operator is configured properly (after the startup activities are done).
    has_admission = bool(registry._resource_webhooks.get_all_handlers())
    if settings.admission.server is None and has_admission:
        raise Exception(
            "Admission handlers exist, but no admission server/tunnel is configured "
            "in `settings.admission.server`. "
            "More: https://kopf.readthedocs.io/en/stable/admission/")

    # Do not start the endpoints until resources are scanned.
    # Otherwise, we generate 404 "Not Found" for requests that arrive too early.
    await insights.ready_resources.wait()

    # Communicate all the client configs the server yields: both the initial one and the updates.
    # On each such change, the configuration manager will wake up and reconfigure the webhooks.
    if settings.admission.server is not None:
        async for client_config in settings.admission.server(webhookfn):
            await container.set(client_config)
    else:
        await asyncio.Event().wait()


