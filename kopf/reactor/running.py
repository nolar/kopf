import asyncio
import functools
import logging
import signal
import threading
import warnings
from typing import Collection, Coroutine, MutableSequence, Optional, Sequence

from kopf.clients import auth
from kopf.engines import peering, posting, probing
from kopf.reactor import activities, admission, daemons, indexing, lifecycles, \
                         observation, orchestration, processing, registries
from kopf.structs import configuration, containers, credentials, ephemera, \
                         handlers, primitives, references, reviews
from kopf.utilities import aiotasks

logger = logging.getLogger(__name__)


def run(
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        lifecycle: Optional[lifecycles.LifeCycleFn] = None,
        indexers: Optional[indexing.OperatorIndexers] = None,
        registry: Optional[registries.OperatorRegistry] = None,
        settings: Optional[configuration.OperatorSettings] = None,
        memories: Optional[containers.ResourceMemories] = None,
        insights: Optional[references.Insights] = None,
        identity: Optional[peering.Identity] = None,
        standalone: Optional[bool] = None,
        priority: Optional[int] = None,
        peering_name: Optional[str] = None,
        liveness_endpoint: Optional[str] = None,
        clusterwide: bool = False,
        namespaces: Collection[references.NamespacePattern] = (),
        namespace: Optional[references.NamespacePattern] = None,  # deprecated
        stop_flag: Optional[primitives.Flag] = None,
        ready_flag: Optional[primitives.Flag] = None,
        vault: Optional[credentials.Vault] = None,
        memo: Optional[ephemera.AnyMemo] = None,
        _command: Optional[Coroutine[None, None, None]] = None,
) -> None:
    """
    Run the whole operator synchronously.

    This function should be used to run an operator in normal sync mode.
    """
    loop = loop if loop is not None else asyncio.get_event_loop()
    try:
        loop.run_until_complete(operator(
            lifecycle=lifecycle,
            indexers=indexers,
            registry=registry,
            settings=settings,
            memories=memories,
            insights=insights,
            identity=identity,
            standalone=standalone,
            clusterwide=clusterwide,
            namespaces=namespaces,
            namespace=namespace,
            priority=priority,
            peering_name=peering_name,
            liveness_endpoint=liveness_endpoint,
            stop_flag=stop_flag,
            ready_flag=ready_flag,
            vault=vault,
            memo=memo,
            _command=_command,
        ))
    except asyncio.CancelledError:
        pass


async def operator(
        *,
        lifecycle: Optional[lifecycles.LifeCycleFn] = None,
        indexers: Optional[indexing.OperatorIndexers] = None,
        registry: Optional[registries.OperatorRegistry] = None,
        settings: Optional[configuration.OperatorSettings] = None,
        memories: Optional[containers.ResourceMemories] = None,
        insights: Optional[references.Insights] = None,
        identity: Optional[peering.Identity] = None,
        standalone: Optional[bool] = None,
        priority: Optional[int] = None,
        peering_name: Optional[str] = None,
        liveness_endpoint: Optional[str] = None,
        clusterwide: bool = False,
        namespaces: Collection[references.NamespacePattern] = (),
        namespace: Optional[references.NamespacePattern] = None,  # deprecated
        stop_flag: Optional[primitives.Flag] = None,
        ready_flag: Optional[primitives.Flag] = None,
        vault: Optional[credentials.Vault] = None,
        memo: Optional[ephemera.AnyMemo] = None,
        _command: Optional[Coroutine[None, None, None]] = None,
) -> None:
    """
    Run the whole operator asynchronously.

    This function should be used to run an operator in an asyncio event-loop
    if the operator is orchestrated explicitly and manually.

    It is efficiently `spawn_tasks` + `run_tasks` with some safety.
    """
    existing_tasks = await aiotasks.all_tasks()
    operator_tasks = await spawn_tasks(
        lifecycle=lifecycle,
        indexers=indexers,
        registry=registry,
        settings=settings,
        memories=memories,
        insights=insights,
        identity=identity,
        standalone=standalone,
        clusterwide=clusterwide,
        namespaces=namespaces,
        namespace=namespace,
        priority=priority,
        peering_name=peering_name,
        liveness_endpoint=liveness_endpoint,
        stop_flag=stop_flag,
        ready_flag=ready_flag,
        vault=vault,
        memo=memo,
        _command=_command,
    )
    await run_tasks(operator_tasks, ignored=existing_tasks)


async def spawn_tasks(
        *,
        lifecycle: Optional[lifecycles.LifeCycleFn] = None,
        indexers: Optional[indexing.OperatorIndexers] = None,
        registry: Optional[registries.OperatorRegistry] = None,
        settings: Optional[configuration.OperatorSettings] = None,
        memories: Optional[containers.ResourceMemories] = None,
        insights: Optional[references.Insights] = None,
        identity: Optional[peering.Identity] = None,
        standalone: Optional[bool] = None,
        priority: Optional[int] = None,
        peering_name: Optional[str] = None,
        liveness_endpoint: Optional[str] = None,
        clusterwide: bool = False,
        namespaces: Collection[references.NamespacePattern] = (),
        namespace: Optional[references.NamespacePattern] = None,  # deprecated
        stop_flag: Optional[primitives.Flag] = None,
        ready_flag: Optional[primitives.Flag] = None,
        vault: Optional[credentials.Vault] = None,
        memo: Optional[ephemera.AnyMemo] = None,
        _command: Optional[Coroutine[None, None, None]] = None,
) -> Collection[aiotasks.Task]:
    """
    Spawn all the tasks needed to run the operator.

    The tasks are properly inter-connected with the synchronisation primitives.
    """
    loop = asyncio.get_running_loop()

    if namespaces and namespace:
        raise TypeError("Either namespaces= or namespace= can be passed. Got both.")
    elif namespace:
        warnings.warn("namespace= is deprecated; use namespaces=[...]", DeprecationWarning)
        namespaces = [namespace]

    if clusterwide and namespaces:
        raise TypeError("The operator can be either cluster-wide or namespaced, not both.")
    if not clusterwide and not namespaces:
        warnings.warn("Absence of either namespaces or cluster-wide flag will become an error soon."
                      " For now, switching to the cluster-wide mode for backward compatibility.",
                      FutureWarning)
        clusterwide = True

    # All tasks of the operator are synced via these primitives and structures:
    lifecycle = lifecycle if lifecycle is not None else lifecycles.get_default_lifecycle()
    registry = registry if registry is not None else registries.get_default_registry()
    settings = settings if settings is not None else configuration.OperatorSettings()
    memories = memories if memories is not None else containers.ResourceMemories()
    indexers = indexers if indexers is not None else indexing.OperatorIndexers()
    insights = insights if insights is not None else references.Insights()
    identity = identity if identity is not None else peering.detect_own_id(manual=False)
    vault = vault if vault is not None else credentials.Vault()
    memo = memo if memo is not None else ephemera.Memo()
    event_queue: posting.K8sEventQueue = asyncio.Queue()
    signal_flag: aiotasks.Future = asyncio.Future()
    started_flag: asyncio.Event = asyncio.Event()
    operator_paused = primitives.ToggleSet(any)
    tasks: MutableSequence[aiotasks.Task] = []

    # Map kwargs into the settings object.
    settings.peering.clusterwide = clusterwide
    if peering_name is not None:
        settings.peering.mandatory = True
        settings.peering.name = peering_name
    if standalone is not None:
        settings.peering.standalone = standalone
    if priority is not None:
        settings.peering.priority = priority

    # Prepopulate indexers with empty indices -- to be available startup handlers.
    indexers.ensure(registry._resource_indexing.get_all_handlers())

    # Global credentials store for this operator, also for CRD-reading & peering mode detection.
    auth.vault_var.set(vault)

    # Special case: pass the settings container through the user-side handlers (no explicit args).
    # Toolkits have to keep the original operator context somehow, and the only way is contextvars.
    posting.settings_var.set(settings)

    # Few common background forever-running infrastructural tasks (irregular root tasks).
    tasks.append(aiotasks.create_task(
        name="stop-flag checker",
        coro=_stop_flag_checker(
            signal_flag=signal_flag,
            stop_flag=stop_flag)))
    tasks.append(aiotasks.create_task(
        name="ultimate termination",
        coro=_ultimate_termination(
            settings=settings,
            stop_flag=stop_flag)))
    tasks.append(aiotasks.create_task(
        name="startup/cleanup activities",
        coro=_startup_cleanup_activities(
            root_tasks=tasks,  # used as a "live" view, populated later.
            ready_flag=ready_flag,
            started_flag=started_flag,
            registry=registry,
            settings=settings,
            indices=indexers.indices,
            vault=vault,
            memo=memo)))  # to purge & finalize the caches in the end.

    # Kill all the daemons gracefully when the operator exits (so that they are not "hung").
    tasks.append(aiotasks.create_guarded_task(
        name="daemon killer", flag=started_flag, logger=logger,
        coro=daemons.daemon_killer(
            settings=settings,
            memories=memories,
            operator_paused=operator_paused)))

    # Keeping the credentials fresh and valid via the authentication handlers on demand.
    tasks.append(aiotasks.create_guarded_task(
        name="credentials retriever", flag=started_flag, logger=logger,
        coro=activities.authenticator(
            registry=registry,
            settings=settings,
            indices=indexers.indices,
            vault=vault,
            memo=memo)))

    # K8s-event posting. Events are queued in-memory and posted in the background.
    # NB: currently, it is a global task, but can be made per-resource or per-object.
    tasks.append(aiotasks.create_guarded_task(
        name="poster of events", flag=started_flag, logger=logger,
        coro=posting.poster(
            backbone=insights.backbone,
            event_queue=event_queue)))

    # Liveness probing -- so that Kubernetes would know that the operator is alive.
    if liveness_endpoint:
        tasks.append(aiotasks.create_guarded_task(
            name="health reporter", flag=started_flag, logger=logger,
            coro=probing.health_reporter(
                registry=registry,
                settings=settings,
                endpoint=liveness_endpoint,
                indices=indexers.indices,
                memo=memo)))

    # Admission webhooks run as either a server or a tunnel or a fixed config.
    # The webhook manager automatically adjusts the cluster configuration at runtime.
    container: primitives.Container[reviews.WebhookClientConfig] = primitives.Container()
    tasks.append(aiotasks.create_guarded_task(
        name="admission insights chain", flag=started_flag, logger=logger,
        coro=primitives.condition_chain(
            source=insights.revised, target=container.changed)))
    tasks.append(aiotasks.create_guarded_task(
        name="admission validating configuration manager", flag=started_flag, logger=logger,
        coro=admission.validating_configuration_manager(
            container=container, settings=settings, registry=registry, insights=insights)))
    tasks.append(aiotasks.create_guarded_task(
        name="admission mutating configuration manager", flag=started_flag, logger=logger,
        coro=admission.mutating_configuration_manager(
            container=container, settings=settings, registry=registry, insights=insights)))
    tasks.append(aiotasks.create_guarded_task(
        name="admission webhook server", flag=started_flag, logger=logger,
        coro=admission.admission_webhook_server(
            container=container, settings=settings, registry=registry, insights=insights,
            webhookfn=functools.partial(admission.serve_admission_request,
                                        settings=settings, registry=registry, insights=insights,
                                        memories=memories, memobase=memo,
                                        indices=indexers.indices))))

    # Permanent observation of what resource kinds and namespaces are available in the cluster.
    # Spawn and cancel dimensional tasks as they come and go; dimensions = resources x namespaces.
    tasks.append(aiotasks.create_guarded_task(
        name="resource observer", flag=started_flag, logger=logger,
        coro=observation.resource_observer(
            insights=insights,
            registry=registry,
            settings=settings)))
    tasks.append(aiotasks.create_guarded_task(
        name="namespace observer", flag=started_flag, logger=logger,
        coro=observation.namespace_observer(
            clusterwide=clusterwide,
            namespaces=namespaces,
            insights=insights,
            settings=settings)))

    # Explicit command is a hack for the CLI to run coroutines in an operator-like environment.
    # If not specified, then use the normal resource processing. It is not exposed publicly (yet).
    if _command is not None:
        tasks.append(aiotasks.create_guarded_task(
            name="the command", flag=started_flag, logger=logger, finishable=True,
            coro=_command))
    else:
        tasks.append(aiotasks.create_guarded_task(
            name="multidimensional multitasker", flag=started_flag, logger=logger,
            coro=orchestration.ochestrator(
                settings=settings,
                insights=insights,
                identity=identity,
                operator_paused=operator_paused,
                processor=functools.partial(processing.process_resource_event,
                                            lifecycle=lifecycle,
                                            registry=registry,
                                            settings=settings,
                                            indexers=indexers,
                                            memories=memories,
                                            memobase=memo,
                                            event_queue=event_queue))))

    # Ensure that all guarded tasks got control for a moment to enter the guard.
    await asyncio.sleep(0)

    # On Ctrl+C or pod termination, cancel all tasks gracefully.
    if threading.current_thread() is threading.main_thread():
        # Handle NotImplementedError when ran on Windows since asyncio only supports Unix signals
        try:
            loop.add_signal_handler(signal.SIGINT, signal_flag.set_result, signal.SIGINT)
            loop.add_signal_handler(signal.SIGTERM, signal_flag.set_result, signal.SIGTERM)
        except NotImplementedError:
            logger.warning("OS signals are ignored: can't add signal handler in Windows.")

    else:
        logger.warning("OS signals are ignored: running not in the main thread.")

    return tasks


async def run_tasks(
        root_tasks: Collection[aiotasks.Task],
        *,
        ignored: Collection[aiotasks.Task] = frozenset(),
) -> None:
    """
    Orchestrate the tasks and terminate them gracefully when needed.

    The root tasks are expected to run forever. Their number is limited. Once
    any of them exits, the whole operator and all other root tasks should exit.

    The root tasks, in turn, can spawn multiple sub-tasks of various purposes.
    They can be awaited, monitored, or fired-and-forgot.

    The hung tasks are those that were spawned during the operator runtime,
    and were not cancelled/exited on the root tasks termination. They are given
    some extra time to finish, after which they are forcely terminated too.

    .. note::
        Due to implementation details, every task created after the operator's
        startup is assumed to be a task or a sub-task of the operator.
        In the end, all tasks are forcely cancelled. Even if those tasks were
        created by other means. There is no way to trace who spawned what.
        Only the tasks that existed before the operator startup are ignored
        (for example, those that spawned the operator itself).
    """

    # Run the infinite tasks until one of them fails/exits (they never exit normally).
    # If the operator is cancelled, propagate the cancellation to all the sub-tasks.
    # There is no graceful period: cancel as soon as possible, but allow them to finish.
    try:
        root_done, root_pending = await aiotasks.wait(root_tasks, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        await aiotasks.stop(root_tasks, title="Root", logger=logger, cancelled=True, interval=10)
        hung_tasks = await aiotasks.all_tasks(ignored=ignored)
        await aiotasks.stop(hung_tasks, title="Hung", logger=logger, cancelled=True, interval=1)
        raise

    # If the operator is intact, but one of the root tasks has exited (successfully or not),
    # cancel all the remaining root tasks, and gracefully exit other spawned sub-tasks.
    root_cancelled, _ = await aiotasks.stop(root_pending, title="Root", logger=logger)

    # After the root tasks are all gone, cancel any spawned sub-tasks (e.g. handlers).
    # If the operator is cancelled, propagate the cancellation to all the sub-tasks.
    # TODO: an assumption! the loop is not fully ours! find a way to cancel only our spawned tasks.
    hung_tasks = await aiotasks.all_tasks(ignored=ignored)
    try:
        hung_done, hung_pending = await aiotasks.wait(hung_tasks, timeout=5)
    except asyncio.CancelledError:
        await aiotasks.stop(hung_tasks, title="Hung", logger=logger, cancelled=True, interval=1)
        raise

    # If the operator is intact, but the timeout is reached, forcely cancel the sub-tasks.
    hung_cancelled, _ = await aiotasks.stop(hung_pending, title="Hung", logger=logger, interval=1)

    # If succeeded or if cancellation is silenced, re-raise from failed tasks (if any).
    await aiotasks.reraise(root_done | root_cancelled | hung_done | hung_cancelled)


async def _stop_flag_checker(
        signal_flag: aiotasks.Future,
        stop_flag: Optional[primitives.Flag],
) -> None:
    """
    A top-level task for external stopping by setting a stop-flag. Once set,
    this task will exit, and thus all other top-level tasks will be cancelled.
    """

    # Selects the flags to be awaited (if set).
    flags = []
    if signal_flag is not None:
        flags.append(signal_flag)
    if stop_flag is not None:
        flags.append(aiotasks.create_task(primitives.wait_flag(stop_flag),
                                          name="stop-flag waiter"))

    # Wait until one of the stoppers is set/raised.
    try:
        done, pending = await asyncio.wait(flags, return_when=asyncio.FIRST_COMPLETED)
        future = done.pop()
        result = await future
    except asyncio.CancelledError:
        pass  # operator is stopping for any other reason
    else:
        if result is None:
            logger.info("Stop-flag is raised. Operator is stopping.")
        elif isinstance(result, signal.Signals):
            logger.info("Signal %s is received. Operator is stopping.", result.name)
        else:
            logger.info("Stop-flag is set to %r. Operator is stopping.", result)


async def _ultimate_termination(
        *,
        settings: configuration.OperatorSettings,
        stop_flag: Optional[primitives.Flag],
) -> None:
    """
    Ensure that SIGKILL is sent regardless of the operator's stopping routines.

    Try to be gentle and kill only the thread with the operator, not the whole
    process or a process group. If this is the main thread (as in most cases),
    this would imply the process termination too.

    Intentional stopping via a stop-flag is ignored.
    """
    # Sleep forever, or until cancelled, which happens when the operator begins its shutdown.
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        if not primitives.check_flag(stop_flag):
            if settings.process.ultimate_exiting_timeout is not None:
                loop = asyncio.get_running_loop()
                loop.call_later(settings.process.ultimate_exiting_timeout,
                                signal.pthread_kill, threading.get_ident(), signal.SIGKILL)


async def _startup_cleanup_activities(
        root_tasks: Sequence[aiotasks.Task],  # mutated externally!
        ready_flag: Optional[primitives.Flag],
        started_flag: asyncio.Event,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        indices: ephemera.Indices,
        vault: credentials.Vault,
        memo: ephemera.AnyMemo,
) -> None:
    """
    Startup and cleanup activities.

    This task spends most of its time in forever sleep, only running
    in the beginning and in the end.

    The root tasks do not actually start until the ready-flag is set,
    which happens after the startup handlers finished successfully.

    Beside calling the startup/cleanup handlers, it performs few operator-scoped
    cleanups too (those that cannot be handled by garbage collection).
    """

    # Execute the startup activity before any root task starts running (due to readiness flag).
    try:
        await activities.run_activity(
            lifecycle=lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            activity=handlers.Activity.STARTUP,
            indices=indices,
            memo=memo,
        )
    except asyncio.CancelledError:
        logger.warning("Startup activity is only partially executed due to cancellation.")
        raise

    # Notify the caller that we are ready to be executed. This unfreezes all the root tasks.
    started_flag.set()
    await primitives.raise_flag(ready_flag)

    # Sleep forever, or until cancelled, which happens when the operator begins its shutdown.
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass

    # Wait for all other root tasks to exit before cleaning up.
    # Beware: on explicit operator cancellation, there is no graceful period at all.
    try:
        current_task = asyncio.current_task()
        awaited_tasks = {task for task in root_tasks if task is not current_task}
        await aiotasks.wait(awaited_tasks)
    except asyncio.CancelledError:
        logger.warning("Cleanup activity is not executed at all due to cancellation.")
        raise

    # Execute the cleanup activity after all other root tasks are presumably done.
    try:
        await activities.run_activity(
            lifecycle=lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            activity=handlers.Activity.CLEANUP,
            indices=indices,
            memo=memo,
        )
        await vault.close()
    except asyncio.CancelledError:
        logger.warning("Cleanup activity is only partially executed due to cancellation.")
        raise
