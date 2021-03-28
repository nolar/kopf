import asyncio
import base64
import copy
import json
import logging
import re
import urllib.parse
from typing import Any, Collection, Dict, Iterable, List, Mapping, Optional

from typing_extensions import Literal, TypedDict

from kopf.clients import creating, errors, patching
from kopf.engines import loggers
from kopf.reactor import causation, handling, lifecycles, registries
from kopf.storage import states
from kopf.structs import bodies, configuration, containers, ephemera, filters, \
                         handlers, ids, patches, primitives, references, reviews

logger = logging.getLogger(__name__)


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
        webhook: Optional[ids.HandlerId] = None,
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

    memory = await memories.recall(raw_body, memo=memobase, ephemeral=operation=='CREATE')
    body = bodies.Body(raw_body)
    patch = patches.Patch()
    warnings: List[str] = []
    cause = causation.ResourceWebhookCause(
        resource=resource,
        indices=indices,
        logger=loggers.LocalObjectLogger(body=body, settings=settings),
        patch=patch,
        memo=memory.memo,
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
        outcomes: Mapping[ids.HandlerId, states.HandlerOutcome],
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


async def validating_configuration_manager(
        *,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        insights: references.Insights,
        container: primitives.Container[reviews.WebhookClientConfig],
) -> None:
    await configuration_manager(
        reason=handlers.WebhookType.VALIDATING,
        selector=references.VALIDATING_WEBHOOK,
        registry=registry, settings=settings,
        insights=insights, container=container,
    )


async def mutating_configuration_manager(
        *,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        insights: references.Insights,
        container: primitives.Container[reviews.WebhookClientConfig],
) -> None:
    await configuration_manager(
        reason=handlers.WebhookType.MUTATING,
        selector=references.MUTATING_WEBHOOK,
        registry=registry, settings=settings,
        insights=insights, container=container,
    )


async def configuration_manager(
        *,
        reason: handlers.WebhookType,
        selector: references.Selector,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        insights: references.Insights,
        container: primitives.Container[reviews.WebhookClientConfig],
) -> None:
    """
    Manage the webhook configurations dynamically.

    This is one of an operator's root tasks that run forever.
    If exited, the whole operator exits as by an error.

    The manager waits for changes in one of these:

    * Observed resources in the cluster (via insights).
    * A new webhook client config yielded by the webhook server.

    On either of these occasion, the manager rebuilds the webhook configuration
    and applies it to the specified configuration resources in the cluster
    (for which it needs some RBAC permissions).
    Besides, it also creates an webhook configuration resource if it is absent.
    """

    # Do nothing if not managed. The root task cannot be skipped from creation,
    # since the managed mode is only set at the startup activities.
    if settings.admission.managed is None:
        await asyncio.Event().wait()
        return

    # Wait until the prerequisites for managing are available (scanned from the cluster).
    await insights.ready_resources.wait()
    resource = await insights.backbone.wait_for(selector)
    all_handlers = registry._resource_webhooks.get_all_handlers()
    all_handlers = [h for h in all_handlers if h.reason == reason]

    # Optionally (if configured), pre-create the configuration objects if they are absent.
    # Use the try-or-fail strategy instead of check-and-do -- to reduce the RBAC requirements.
    try:
        await creating.create_obj(resource=resource, name=settings.admission.managed)
    except errors.APIConflictError:
        pass  # exists already
    except errors.APIForbiddenError:
        logger.error(f"Not enough RBAC permissions to create a {resource}.")
        raise

    # Execute either when actually changed (yielded from the webhook server),
    # or the condition is chain-notified (from the insights: on resources/namespaces revision).
    # Ignore inconsistencies: they are expected -- the server fills the defaults.
    client_config: Optional[reviews.WebhookClientConfig] = None
    try:
        async for client_config in container.as_changed():
            logger.info(f"Reconfiguring the {reason.value} webhook {settings.admission.managed}.")
            webhooks = build_webhooks(
                all_handlers,
                resources=insights.resources,
                name_suffix=settings.admission.managed,
                client_config=client_config)
            await patching.patch_obj(
                resource=resource,
                namespace=None,
                name=settings.admission.managed,
                patch=patches.Patch({'webhooks': webhooks}),
            )
    finally:
        # Attempt to remove all managed webhooks, except for the strict ones.
        if client_config is not None:
            logger.info(f"Cleaning up the admission webhook {settings.admission.managed}.")
            webhooks = build_webhooks(
                all_handlers,
                resources=insights.resources,
                name_suffix=settings.admission.managed,
                client_config=client_config,
                persistent_only=True)
            await patching.patch_obj(
                resource=resource,
                namespace=None,
                name=settings.admission.managed,
                patch=patches.Patch({'webhooks': webhooks}),
            )


def build_webhooks(
        handlers_: Iterable[handlers.ResourceWebhookHandler],
        *,
        resources: Iterable[references.Resource],
        name_suffix: str,
        client_config: reviews.WebhookClientConfig,
        persistent_only: bool = False,
) -> List[Dict[str, Any]]:
    """
    Construct the content for ``[Validating|Mutating]WebhookConfiguration``.

    This function concentrates all conventions how Kopf manages the webhook.
    """
    return [
        {
            'name': _normalize_name(handler.id, suffix=name_suffix),
            'sideEffects': 'NoneOnDryRun' if handler.side_effects else 'None',
            'failurePolicy': 'Ignore' if handler.ignore_failures else 'Fail',
            'matchPolicy': 'Equivalent',
            'rules': [
                {
                    'apiGroups': [resource.group],
                    'apiVersions': [resource.version],
                    'resources': [resource.plural],
                    'operations': ['*'] if handler.operation is None else [handler.operation],
                    'scope': '*',  # doesn't matter since a specific resource is used.
                }
                for resource in resources
                if handler.selector is not None  # None is used only in sub-handlers, ignore here.
                if handler.selector.check(resource)
            ],
            'objectSelector': _build_labels_selector(handler.labels),
            'clientConfig': _inject_handler_id(client_config, handler.id),
            'timeoutSeconds': 30,  # a permitted maximum is 30.
            'admissionReviewVersions': ['v1', 'v1beta1'],  # only those understood by Kopf itself.
        }
        for handler in handlers_
        if not persistent_only or handler.persistent
    ]


class MatchExpression(TypedDict, total=False):
    key: str
    operator: Literal['Exists', 'DoesNotExist', 'In', 'NotIn']
    values: Optional[Collection[str]]


def _build_labels_selector(labels: Optional[filters.MetaFilter]) -> Optional[Mapping[str, Any]]:
    # https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#resources-that-support-set-based-requirements
    exprs: Collection[MatchExpression] = [
        {'key': key, 'operator': 'Exists'} if val is filters.MetaFilterToken.PRESENT else
        {'key': key, 'operator': 'DoesNotExist'} if val is filters.MetaFilterToken.ABSENT else
        {'key': key, 'operator': 'In', 'values': [str(val)]}
        for key, val in (labels or {}).items()
        if not callable(val)
    ]
    return {'matchExpressions': exprs} if exprs else None


BAD_WEBHOOK_NAME = re.compile(r'[^\w\d\.-]')


def _normalize_name(id: ids.HandlerId, suffix: str) -> str:
    """
    Normalize the webhook name to what Kubernetes accepts as normal.

    The restriction is: *a lowercase RFC 1123 subdomain must consist
    of lower case alphanumeric characters, \'-\' or \'.\',
    and must start and end with an alphanumeric character.*

    The actual name is not that important, it is for informational purposes
    only. In the managed configurations, it will be rewritten every time.
    """
    name = f'{id}'.replace('/', '.').replace('_', '-')  # common cases, for beauty
    name = BAD_WEBHOOK_NAME.sub(lambda s: s.group(0).encode('utf-8').hex(), name)  # uncommon cases
    return f'{name}.{suffix}' if suffix else name


def _inject_handler_id(config: reviews.WebhookClientConfig, id: ids.HandlerId) -> reviews.WebhookClientConfig:
    config = copy.deepcopy(config)

    url_id = urllib.parse.quote(id)
    url = config.get('url')
    if url is not None:
        config['url'] = f'{url.rstrip("/")}/{url_id}'

    service = config.get('service')
    if service is not None:
        path = service.get('path', '')
        service['path'] = f"{path}/{url_id}"

    return config
