"""
Orchestrating the tasks for served resources & namespaces.

The resources & namespaces are observed in :mod:`.observation`, where they
are stored in the "insights" --- a description of the current cluster setup.
They are used as the input for the orchestration.

For every combination of every actual resource & every actual namespace,
there is a watcher task and a few optional peering tasks/toggles.
The tasks are started when new values are added to any of these dimension,
or stopped when some existing values are removed.

There are several kinds of tasks:

* Regular watchers (watch-streams) --- the main one.
* Peering watchers (watch-streams).
* Peering keep-alives (pingers).

The peering tasks are started only when the peering is enabled at all.
For peering, the resource is not used, only the namespace is of importance.

Some special watchers for the meta-level resources --- i.e. for dimensions ---
are started and stopped separately, not as part of the orchestration.
"""
import asyncio
import contextlib
import dataclasses
import functools
import itertools
import logging
from collections.abc import Collection, Container, Iterable
from typing import Any, NamedTuple, Protocol

from kopf._cogs.aiokits import aiotasks, aiotoggles
from kopf._cogs.configs import configuration
from kopf._cogs.structs import bodies, references
from kopf._core.engines import peering
from kopf._core.reactor import queueing

logger = logging.getLogger(__name__)


class EnsembleKey(NamedTuple):
    resource: references.Resource
    namespace: references.Namespace


# Differs from queueing.WatchStreamProcessor by the resource=… kwarg.
class ResourceWatchStreamProcessor(Protocol):
    async def __call__(
            self,
            *,
            resource: references.Resource,
            raw_event: bodies.RawEvent,
            stream_pressure: asyncio.Event | None = None,  # None for tests
            resource_indexed: aiotoggles.Toggle | None = None,  # None for tests & observation
            operator_indexed: aiotoggles.ToggleSet | None = None,  # None for tests & observation
    ) -> str | None:
        ...


@dataclasses.dataclass
class Ensemble:

    # Global synchronisation point on the cache pre-populating stage and overall cache readiness.
    # Note: there is no need for ToggleSet; it is checked by emptiness of items inside.
    #       ToggleSet is used because it is the closest equivalent of such a primitive.
    operator_indexed: aiotoggles.ToggleSet

    # Multidimensional pausing: for every namespace, and a few for the whole cluster (for CRDs).
    operator_paused: aiotoggles.ToggleSet
    peering_missing: aiotoggles.Toggle
    conflicts_found: dict[EnsembleKey, aiotoggles.Toggle] = dataclasses.field(default_factory=dict)
    resource_indexed: dict[EnsembleKey, aiotoggles.Toggle] = dataclasses.field(default_factory=dict)

    # Multidimensional tasks -- one for every combination of relevant dimensions.
    watcher_tasks: dict[EnsembleKey, aiotasks.Task] = dataclasses.field(default_factory=dict)
    peering_tasks: dict[EnsembleKey, aiotasks.Task] = dataclasses.field(default_factory=dict)
    pinging_tasks: dict[EnsembleKey, aiotasks.Task] = dataclasses.field(default_factory=dict)

    def get_keys(self) -> Collection[EnsembleKey]:
        return (frozenset(self.watcher_tasks) |
                frozenset(self.peering_tasks) |
                frozenset(self.pinging_tasks) |
                frozenset(self.conflicts_found) |
                frozenset(self.resource_indexed))

    def get_tasks(self, keys: Container[EnsembleKey]) -> Collection[aiotasks.Task]:
        return {task
                for tasks in [self.watcher_tasks, self.peering_tasks, self.pinging_tasks]
                for key, task in tasks.items() if key in keys}

    def get_flags(self, keys: Container[EnsembleKey]) -> Collection[aiotoggles.Toggle]:
        return {toggle for key, toggle in self.conflicts_found.items() if key in keys}

    def get_indexing_flags(self, keys: Container[EnsembleKey]) -> Collection[aiotoggles.Toggle]:
        return {toggle for key, toggle in self.resource_indexed.items() if key in keys}

    def del_keys(self, keys: Container[EnsembleKey]) -> None:
        d: dict[EnsembleKey, Any]
        for d in [self.watcher_tasks, self.peering_tasks, self.pinging_tasks]:
            for key in set(d):
                if key in keys:
                    del d[key]
        for d in [self.conflicts_found, self.resource_indexed]:  # for easier type inference
            for key in set(d):
                if key in keys:
                    del d[key]


async def orchestrator(
        *,
        processor: ResourceWatchStreamProcessor,
        settings: configuration.OperatorSettings,
        identity: peering.Identity,
        insights: references.Insights,
        operator_paused: aiotoggles.ToggleSet,
) -> None:
    peering_missing = await operator_paused.make_toggle(name='peering CRD is missing')
    ensemble = Ensemble(
        peering_missing=peering_missing,
        operator_paused=operator_paused,
        operator_indexed=aiotoggles.ToggleSet(all),
    )
    try:
        async with insights.revised:
            while True:
                ensemble_tasks = set(ensemble.get_tasks(ensemble.get_keys()))
                insights_waiter = asyncio.create_task(
                    insights.revised.wait(),
                    name="insights revision waiter",
                )
                try:
                    done, _ = await aiotasks.wait(
                        ensemble_tasks | {insights_waiter},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    await aiotasks.stop(
                        {insights_waiter},
                        title="insights revision waiter",
                        quiet=True,
                    )

                ensemble_done = done & ensemble_tasks
                await aiotasks.reraise(ensemble_done)
                if ensemble_done:
                    await recycle_terminated_tasks(ensemble=ensemble, tasks=ensemble_done)

                    # Back off with the insights unlocked, but never unlock them between
                    # the iterations: a revision notified there would have no waiter and be lost.
                    # The revisions made during the backoff are applied by adjust_tasks() below.
                    with contextlib.suppress(TimeoutError):
                        async with asyncio.timeout(settings.watching.reconnect_backoff):
                            while True:
                                await insights.revised.wait()

                    delayed_done = {
                        task
                        for task in ensemble.get_tasks(ensemble.get_keys())
                        if task.done()
                    }
                    await aiotasks.reraise(delayed_done)
                    if delayed_done:
                        await recycle_terminated_tasks(ensemble=ensemble, tasks=delayed_done)

                await adjust_tasks(
                    processor=processor,
                    insights=insights,
                    settings=settings,
                    identity=identity,
                    ensemble=ensemble,
                )
    except asyncio.CancelledError:
        tasks = ensemble.get_tasks(ensemble.get_keys())
        await aiotasks.stop(tasks, title="streaming", logger=logger, interval=10)
        raise


async def recycle_terminated_tasks(
        *,
        ensemble: Ensemble,
        tasks: Container[aiotasks.Task],
) -> None:
    watcher_keys = {key for key, task in ensemble.watcher_tasks.items() if task in tasks}
    peering_keys = {
        key
        for task_map in [ensemble.peering_tasks, ensemble.pinging_tasks]
        for key, task in task_map.items()
        if task in tasks
    }
    peering_tasks = {
        task
        for task_map in [ensemble.peering_tasks, ensemble.pinging_tasks]
        for key, task in task_map.items()
        if key in peering_keys
    }
    await aiotasks.stop(peering_tasks, title="streaming", logger=logger, interval=10, quiet=True)

    for key in watcher_keys:
        where = f'in {key.namespace!r}' if key.namespace is not None else 'cluster-wide'
        logger.debug(f"Watcher for {key.resource} {where} ended; reconciling after backoff.")
        ensemble.watcher_tasks.pop(key, None)
    for key in peering_keys:
        ensemble.peering_tasks.pop(key, None)
        ensemble.pinging_tasks.pop(key, None)
    await aiotasks.reraise(peering_tasks)
    for key in peering_keys:
        where = f'in {key.namespace!r}' if key.namespace is not None else 'cluster-wide'
        logger.debug(f"Peering task for {key.resource} {where} ended; reconciling after backoff.")


# Directly corresponds to one iteration of an orchestrator, but it is extracted for testability:
# for a simulation of the insights (inputs) and an assertion of the tasks & toggles (outputs).
async def adjust_tasks(
        *,
        processor: ResourceWatchStreamProcessor,
        insights: references.Insights,
        settings: configuration.OperatorSettings,
        identity: peering.Identity,
        ensemble: Ensemble,
) -> None:
    peering_selectors = peering.guess_selectors(settings=settings)
    peering_resources = {insights.backbone[s] for s in peering_selectors if s in insights.backbone}

    # Pause or resume all streams if the peering CRDs are absent but required.
    # Ignore the CRD absence in auto-detection mode: pause only when (and if) the CRDs are added.
    await ensemble.peering_missing.turn_to(settings.peering.mandatory and not peering_resources)

    # Stop & start the tasks to match the task matrix with the cluster insights.
    # As a rule of thumb, stop the tasks first, start later -- not vice versa!
    await terminate_redundancies(ensemble=ensemble,
                                 remaining_resources=insights.watched_resources | peering_resources,
                                 remaining_namespaces=insights.namespaces | {None})
    await spawn_missing_peerings(ensemble=ensemble,
                                 settings=settings,
                                 identity=identity,
                                 resources=peering_resources,
                                 namespaces=insights.namespaces)
    await spawn_missing_watchers(ensemble=ensemble,
                                 settings=settings,
                                 processor=processor,
                                 indexed_resources=insights.indexed_resources,
                                 watched_resources=insights.watched_resources,
                                 watched_namespaces=insights.namespaces)


async def terminate_redundancies(
        *,
        remaining_resources: Collection[references.Resource],
        remaining_namespaces: Collection[references.Namespace],
        ensemble: Ensemble,
) -> None:
    # Do not distinguish the keys: even for the case when the peering CRD is served by the operator,
    # for the peering CRD or namespace deletion, both tasks are stopped together, never apart.
    redundant_keys = {key for key in ensemble.get_keys()
                      if key.namespace not in remaining_namespaces
                      or key.resource not in remaining_resources}
    redundant_tasks = ensemble.get_tasks(redundant_keys)
    redundant_flags = ensemble.get_flags(redundant_keys)
    redundant_indexing_flags = ensemble.get_indexing_flags(redundant_keys)
    await aiotasks.stop(redundant_tasks, title="streaming", logger=logger, interval=10, quiet=True)
    await ensemble.operator_paused.drop_toggles(redundant_flags)
    await ensemble.operator_indexed.drop_toggles(redundant_indexing_flags)
    ensemble.del_keys(redundant_keys)


async def spawn_missing_peerings(
        *,
        settings: configuration.OperatorSettings,
        identity: peering.Identity,
        resources: Collection[references.Resource],
        namespaces: Collection[references.Namespace],
        ensemble: Ensemble,
) -> None:
    for resource, namespace in itertools.product(resources, namespaces):
        namespace = namespace if resource.namespaced else None
        dkey = EnsembleKey(resource=resource, namespace=namespace)
        if dkey not in ensemble.peering_tasks:
            what = f"{settings.peering.name}@{namespace}"
            conflicts_found = ensemble.conflicts_found.get(dkey)
            if conflicts_found is None:
                is_preactivated = settings.peering.mandatory
                conflicts_found = await ensemble.operator_paused.make_toggle(
                    is_preactivated, name=what)
                ensemble.conflicts_found[dkey] = conflicts_found
            ensemble.pinging_tasks[dkey] = aiotasks.create_guarded_task(
                name=f"peering keep-alive for {what}", logger=logger, cancellable=True,
                coro=peering.keepalive(
                    namespace=namespace,
                    resource=resource,
                    settings=settings,
                    identity=identity))
            ensemble.peering_tasks[dkey] = aiotasks.create_guarded_task(
                name=f"peering observer for {what}", logger=logger, cancellable=True,
                coro=queueing.watcher(
                    settings=settings,
                    resource=resource,
                    namespace=namespace,
                    processor=functools.partial(peering.process_peering_event,
                                                conflicts_found=conflicts_found,
                                                namespace=namespace,
                                                resource=resource,
                                                settings=settings,
                                                identity=identity)))

    # Ensure that all guarded tasks got control for a moment to enter the guard.
    await asyncio.sleep(0)


async def spawn_missing_watchers(
        *,
        processor: ResourceWatchStreamProcessor,
        settings: configuration.OperatorSettings,
        indexed_resources: Container[references.Resource],  # only "if in", never "for in"!
        watched_resources: Iterable[references.Resource],
        watched_namespaces: Iterable[references.Namespace],
        ensemble: Ensemble,
) -> None:

    # Block the operator globally until specialised per-resource-kind blockers are created.
    # NB: Must be created before the point of parallelisation!
    operator_blocked = await ensemble.operator_indexed.make_toggle(name="orchestration blocker")

    # Spawn watchers and create the specialised per-resource-kind blockers.
    for resource, namespace in itertools.product(watched_resources, watched_namespaces):
        namespace = namespace if resource.namespaced else None
        dkey = EnsembleKey(resource=resource, namespace=namespace)
        resource_indexed = ensemble.resource_indexed.get(dkey)
        if dkey not in ensemble.watcher_tasks:
            what = f"{resource}@{namespace}"
            if resource in indexed_resources and resource_indexed is None:
                resource_indexed = await ensemble.operator_indexed.make_toggle(name=what)
                ensemble.resource_indexed[dkey] = resource_indexed
            ensemble.watcher_tasks[dkey] = aiotasks.create_guarded_task(
                name=f"watcher for {what}", logger=logger, cancellable=True,
                coro=queueing.watcher(
                    operator_paused=ensemble.operator_paused,
                    operator_indexed=ensemble.operator_indexed,
                    resource_indexed=resource_indexed,
                    settings=settings,
                    resource=resource,
                    namespace=namespace,
                    processor=functools.partial(processor, resource=resource)))

    # Unblock globally, let the specialised per-resource-kind blockers hold the readiness.
    await ensemble.operator_indexed.drop_toggle(operator_blocked)

    # Ensure that all guarded tasks got control for a moment to enter the guard.
    await asyncio.sleep(0)
