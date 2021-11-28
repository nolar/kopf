"""
Keeping track of the cluster setup: namespaces, resources (custom and builtin).

The outcome of observation are "insights" -- a description of the cluster setup,
including the "backbone" -- core resources to be used by the operator/framework.

The resource specifications can be partial or even fuzzy (e.g. by categories),
with zero, one, or more actual resources matching the specification (selector).
Similarly, the namespaces can be specified by patterns or negations of them.

The actual resources & namespaces in the cluster are permanently observed
and matched against the specifications. Only those that do match are served.

If there are no permissions to observe the CRDs or namespaces, the framework
attempts the best possible fallback scenario:

* For CRDs, only the initial scan is done; no runtime observation is performed.
* For namespaces, the namespace patterns are treated as exact namespace names.

A warning is logged unless ``settings.scanning.disabled`` is set to true
to declare this restricted mode as the desired mode of operation.
"""
import asyncio
import functools
import logging
from typing import Collection, FrozenSet, Iterable, List, Optional, Set

from kopf._cogs.aiokits import aiotoggles
from kopf._cogs.clients import errors, fetching, scanning
from kopf._cogs.configs import configuration
from kopf._cogs.structs import bodies, references
from kopf._core.intents import handlers, registries
from kopf._core.reactor import queueing

logger = logging.getLogger(__name__)


async def namespace_observer(
        *,
        clusterwide: bool,
        namespaces: Collection[references.NamespacePattern],
        insights: references.Insights,
        settings: configuration.OperatorSettings,
) -> None:
    exact_namespaces = references.select_specific_namespaces(namespaces)
    resource = await insights.backbone.wait_for(references.NAMESPACES)

    # Populate the namespaces atomically (instead of notifying on every item from the watch-stream).
    if not settings.scanning.disabled and not clusterwide:
        try:
            objs, _ = await fetching.list_objs(
                settings=settings,
                resource=resource,
                namespace=None,
                logger=logger,
            )
            async with insights.revised:
                revise_namespaces(raw_bodies=objs, insights=insights, namespaces=namespaces)
                insights.revised.notify_all()
        except errors.APIForbiddenError:
            logger.warning("Not enough permissions to list namespaces. "
                           "Falling back to a list of namespaces which are assumed to exist: "
                           f"{exact_namespaces!r}")
            async with insights.revised:
                insights.namespaces.update(exact_namespaces)
                insights.revised.notify_all()
    else:
        async with insights.revised:
            insights.namespaces.update({None} if clusterwide else exact_namespaces)
            insights.revised.notify_all()

    # Notify those waiting for the initial listing (e.g. CLI commands).
    insights.ready_namespaces.set()

    if not settings.scanning.disabled and not clusterwide:
        try:
            await queueing.watcher(
                settings=settings,
                resource=resource,
                namespace=None,
                processor=functools.partial(process_discovered_namespace_event,
                                            namespaces=namespaces,
                                            insights=insights))
        except errors.APIForbiddenError:
            logger.warning("Not enough permissions to watch for namespaces: "
                           "changes (deletion/creation) will not be noticed; "
                           "the namespaces are only refreshed on operator restarts.")
            await asyncio.Event().wait()
    else:
        await asyncio.Event().wait()


async def resource_observer(
        *,
        settings: configuration.OperatorSettings,
        registry: registries.OperatorRegistry,
        insights: references.Insights,
) -> None:

    # Scan only the resource-related handlers, ignore activies & co.
    all_handlers: List[handlers.ResourceHandler] = []
    all_handlers.extend(registry._webhooks.get_all_handlers())
    all_handlers.extend(registry._indexing.get_all_handlers())
    all_handlers.extend(registry._watching.get_all_handlers())
    all_handlers.extend(registry._spawning.get_all_handlers())
    all_handlers.extend(registry._changing.get_all_handlers())
    groups = {handler.selector.group for handler in all_handlers if handler.selector is not None}
    groups.update({selector.group for selector in insights.backbone.selectors})

    # Prepopulate the resources before the dimension watchers start, so that each initially listed
    # namespace would start a watcher, and each initially listed CRD is already on the list.
    group_filter = None if None in groups else {group for group in groups if group is not None}
    resources = await scanning.scan_resources(groups=group_filter, settings=settings, logger=logger)
    async with insights.revised:
        revise_resources(resources=resources, insights=insights, registry=registry, group=None)
        await insights.backbone.fill(resources=resources)
        insights.revised.notify_all()

    # Notify those waiting for the initial listing (e.g. CLI commands).
    insights.ready_resources.set()

    # The resource watcher starts with an initial listing, and later reacts to any changes. However,
    # the existing resources are known already, so there will be no changes on the initial listing.
    resource = await insights.backbone.wait_for(references.CRDS)
    if not settings.scanning.disabled:
        try:
            await queueing.watcher(
                settings=settings,
                resource=resource,
                namespace=None,
                processor=functools.partial(process_discovered_resource_event,
                                            settings=settings,
                                            registry=registry,
                                            insights=insights))
        except errors.APIForbiddenError:
            logger.warning("Not enough permissions to watch for resources: "
                           "changes (creation/deletion/updates) will not be noticed; "
                           "the resources are only refreshed on operator restarts.")
            await asyncio.Event().wait()
    else:
        await asyncio.Event().wait()


async def process_discovered_namespace_event(
        *,
        raw_event: bodies.RawEvent,
        namespaces: Collection[references.NamespacePattern],
        insights: references.Insights,
        # Must be accepted whether used or not -- as passed by watcher()/worker().
        stream_pressure: Optional[asyncio.Event] = None,  # None for tests
        resource_indexed: Optional[aiotoggles.Toggle] = None,  # None for tests & observation
        operator_indexed: Optional[aiotoggles.ToggleSet] = None,  # None for tests & observation
) -> None:
    if raw_event['type'] is None:
        return

    async with insights.revised:
        revise_namespaces(raw_events=[raw_event], insights=insights, namespaces=namespaces)
        insights.revised.notify_all()


async def process_discovered_resource_event(
        *,
        raw_event: bodies.RawEvent,
        settings: configuration.OperatorSettings,
        registry: registries.OperatorRegistry,
        insights: references.Insights,
        # Must be accepted whether used or not -- as passed by watcher()/worker().
        stream_pressure: Optional[asyncio.Event] = None,  # None for tests
        resource_indexed: Optional[aiotoggles.Toggle] = None,  # None for tests & observation
        operator_indexed: Optional[aiotoggles.ToggleSet] = None,  # None for tests & observation
) -> None:
    # Ignore the initial listing, as all custom resources were already noticed by API listing.
    # This prevents numerous unneccessary API requests at the the start of the operator.
    if raw_event['type'] is None:
        return

    # Re-scan the whole dimension of resources if any single one of them changes. By this, we make
    # K8s's /apis/ endpoint the source of truth for all resources & versions & preferred versions,
    # instead of mimicking K8s in interpreting them ourselves (a probable source of bugs).
    # As long as it is atomic (for asyncio, i.e. sync), the existing tasks will not be affected.
    group = raw_event['object']['spec']['group']
    resources = await scanning.scan_resources(groups={group}, settings=settings, logger=logger)
    async with insights.revised:
        revise_resources(resources=resources, insights=insights, registry=registry, group=group)
        await insights.backbone.fill(resources=resources)
        insights.revised.notify_all()


def revise_namespaces(
        *,
        insights: references.Insights,
        namespaces: Collection[references.NamespacePattern],
        raw_events: Collection[bodies.RawEvent] = (),
        raw_bodies: Collection[bodies.RawBody] = (),
) -> None:
    all_events = list(raw_events) + [bodies.RawEvent(type=None, object=obj) for obj in raw_bodies]
    for raw_event in all_events:
        namespace = references.NamespaceName(raw_event['object']['metadata']['name'])
        matched = any(references.match_namespace(namespace, pattern) for pattern in namespaces)
        deleted = is_deleted(raw_event)
        if deleted:
            insights.namespaces.discard(namespace)
        elif matched:
            insights.namespaces.add(namespace)


def revise_resources(
        *,
        group: Optional[str],
        insights: references.Insights,
        registry: registries.OperatorRegistry,
        resources: Collection[references.Resource],
) -> None:

    # Scan only the resource-related handlers grouped by purpose; ignore activities & co.
    webhook_selectors = registry._webhooks.get_all_selectors()
    indexed_selectors = registry._indexing.get_all_selectors()
    watched_selectors = (
        registry._indexing.get_all_selectors() |
        registry._watching.get_all_selectors() |
        registry._spawning.get_all_selectors() |
        registry._changing.get_all_selectors()
    )
    patched_selectors = (
        registry._spawning.get_all_selectors() |
        registry._changing.get_all_selectors()
    )

    # Note: indexed and webhook resources are not checked for ambiguity or empty matching:
    # - the indexed resources are not served/watched directly and are only used as an utility cache;
    # - the webhook resources are PASSIVELY matched per HTTP request, so ambiguity is not a problem.
    # Ambiguity is a potential problem only for regular resource handlers because the operators
    # ACTIVELY trigger them and produce irreversible side-effects --- even if improperly configured.
    _update_resources(insights.webhook_resources, webhook_selectors, group=group, source=resources)
    _update_resources(insights.indexed_resources, indexed_selectors, group=group, source=resources)
    _update_resources(insights.watched_resources, watched_selectors, group=group, source=resources)
    _disable_ambiguous_selectors(resources=insights.watched_resources, selectors=watched_selectors)
    _disable_mismatched_selectors(resources=insights.watched_resources, selectors=watched_selectors)
    _disable_unsuitable_resources(resources=insights.watched_resources, selectors=patched_selectors)


def _update_resources(
        resources: Set[references.Resource],
        selectors: Iterable[references.Selector],
        *,
        group: Optional[str],
        source: Collection[references.Resource],
) -> None:
    """
    Update all or the group's resources from the source of resources.

    This also excludes the resources that continue to exist but stop matching
    the selectors: e.g. by category --- if a CRD's categories were modified.

    WARNING: We do not block the CRDs by adding finalizers (for simplicity),
    so it can be so that we miss the CRD deletion event and continue
    the watching attempts (and fail with HTTP 404).
    """

    # Exclude previously served resources that are gone now.
    group_resources = {resource for resource in resources if group in [None, resource.group]}
    resources.difference_update(group_resources)

    # Include or re-include the resources that are [still] served.
    for selector in selectors:
        resources.update(selector.select(source))


def _disable_ambiguous_selectors(
        *,
        resources: Set[references.Resource],
        selectors: Iterable[references.Selector],
) -> None:
    """
    Detect ambiguous selectors and stop serving/watching them.

    Ambiguous selectors are those matching 2+ distinct resources.
    For example, if a selector specifies "pods" and there are resources
    "pods.v1" and "pods.v1beta1.metrics.k8s.io" (but only if non-v1 resources
    cannot be filtered out completely; otherwise, implicitly prefer v1).
    """
    for selector in selectors:
        selected = selector.select(resources)
        if selector.is_specific and len(selected) > 1:
            logger.warning("Ambiguous resources will not be served (try specifying API groups):"
                           f" {selector} => {selected}")
            resources.difference_update(selected)


def _disable_mismatched_selectors(
        *,
        resources: Set[references.Resource],
        selectors: FrozenSet[references.Selector],
) -> None:
    """
    Warn for handlers that specify nonexistent resources.

    This can be due to a typo or a misconfiguration or CRDs are not yet created.
    """
    selector_names = ", ".join(
        f"{selector}"
        for selector in selectors
        if not selector.select(resources)
    )
    if selector_names:
        logger.warning("Unresolved resources cannot be served (try creating their CRDs):"
                       f" {selector_names}")


def _disable_unsuitable_resources(
        *,
        resources: Set[references.Resource],
        selectors: FrozenSet[references.Selector],
) -> None:

    # For both watching & patching, only look at watched resources, ignore webhook-only resources.
    nonwatchable_resources = {resource for resource in resources
                              if 'watch' not in resource.verbs or 'list' not in resource.verbs}
    nonpatchable_resources = {resource for resource in resources
                              if 'patch' not in resource.verbs} - nonwatchable_resources

    # For patching, only react if there are handlers that store a state (i.e. not on-event/index).
    patching_required = any(selector.select(nonpatchable_resources) for selector in selectors)

    if nonwatchable_resources:
        logger.warning(f"Non-watchable resources will not be served: {nonwatchable_resources}")
        resources.difference_update(nonwatchable_resources)
    if nonpatchable_resources and patching_required:
        logger.warning(f"Non-patchable resources will not be served: {nonpatchable_resources}")
        resources.difference_update(nonpatchable_resources)


def is_deleted(raw_event: bodies.RawEvent) -> bool:
    marked_as_deleted = bool(raw_event['object'].get('metadata', {}).get('deletionTimestamp'))
    really_is_deleted = raw_event['type'] == 'DELETED'
    return marked_as_deleted or really_is_deleted
