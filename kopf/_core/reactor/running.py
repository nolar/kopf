import asyncio
import functools
import logging
import signal
import threading
import warnings
from collections.abc import Collection, Coroutine, MutableSequence, Sequence
from typing import Any

from kopf._cogs.aiokits import aioadapters, aiobindings, aiotasks, aiotoggles, aiovalues
from kopf._cogs.clients import auth
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import versions
from kopf._cogs.structs import credentials, ephemera, references, reviews
from kopf._core.actions import execution, lifecycles
from kopf._core.engines import activities, admission, daemons, indexing, peering, posting, probing
from kopf._core.intents import causes, registries
from kopf._core.reactor import inventory, observation, orchestration, processing

logger = logging.getLogger(__name__)


def run(
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        lifecycle: execution.LifeCycleFn | None = None,
        indexers: indexing.OperatorIndexers | None = None,
        registry: registries.OperatorRegistry | None = None,
        settings: configuration.OperatorSettings | None = None,
        memories: inventory.ResourceMemories | None = None,
        insights: references.Insights | None = None,
        identity: peering.Identity | None = None,
        standalone: bool | None = None,
        priority: int | None = None,
        peering_name: str | None = None,
        liveness_endpoint: str | None = None,
        clusterwide: bool = False,
        namespaces: Collection[references.NamespacePattern] = (),
        namespace: references.NamespacePattern | None = None,  # deprecated
        stop_flag: aioadapters.Flag | None = None,
        ready_flag: aioadapters.Flag | None = None,
        vault: credentials.Vault | None = None,
        memo: object | None = None,
        _command: Coroutine[None, None, None] | None = None,
) -> None:
    """
    Run the whole operator synchronously.

    If the loop is not specified, the operator runs in the event loop
    of the current _context_ (by asyncio's default, the current thread).
    See: https://docs.python.org/3/library/asyncio-policy.html for details.

    Alternatively, use ``asyncio.run(kopf.operator(...))`` with the same args.
    It will take care of a new event loop's creation and finalization for this
    call. See: :func:`asyncio.run`.
    """
    coro = operator(
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
    try:
        if loop is not None:
            loop.run_until_complete(coro)
        else:
            asyncio.run(coro)
    except asyncio.CancelledError:
        pass


async def operator(
        *,
        lifecycle: execution.LifeCycleFn | None = None,
        indexers: indexing.OperatorIndexers | None = None,
        registry: registries.OperatorRegistry | None = None,
        settings: configuration.OperatorSettings | None = None,
        memories: inventory.ResourceMemories | None = None,
        insights: references.Insights | None = None,
        identity: peering.Identity | None = None,
        standalone: bool | None = None,
        priority: int | None = None,
        peering_name: str | None = None,
        liveness_endpoint: str | None = None,
        clusterwide: bool = False,
        namespaces: Collection[references.NamespacePattern] = (),
        namespace: references.NamespacePattern | None = None,  # deprecated
        stop_flag: aioadapters.Flag | None = None,
        ready_flag: aioadapters.Flag | None = None,
        vault: credentials.Vault | None = None,
        memo: object | None = None,
        _command: Coroutine[None, None, None] | None = None,
) -> None:
    """
    Run the whole operator asynchronously.

    This function should be used to run an operator in an asyncio event-loop
    if the operator is orchestrated explicitly and manually.

    It is effectively :func:`spawn_tasks` + :func:`run_tasks` with some safety.
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
        lifecycle: execution.LifeCycleFn | None = None,
        indexers: indexing.OperatorIndexers | None = None,
        registry: registries.OperatorRegistry | None = None,
        settings: configuration.OperatorSettings | None = None,
        memories: inventory.ResourceMemories | None = None,
        insights: references.Insights | None = None,
        identity: peering.Identity | None = None,
        standalone: bool | None = None,
        priority: int | None = None,
        peering_name: str | None = None,
        liveness_endpoint: str | None = None,
        clusterwide: bool = False,
        namespaces: Collection[references.NamespacePattern] = (),
        namespace: references.NamespacePattern | None = None,  # deprecated
        stop_flag: aioadapters.Flag | None = None,
        ready_flag: aioadapters.Flag | None = None,
        vault: credentials.Vault | None = None,
        memo: object | None = None,
        flat: bool = False,  # for backwards compatibility, just in case
        _command: Coroutine[None, None, None] | None = None,
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
    memories = memories if memories is not None else inventory.ResourceMemories()
    indexers = indexers if indexers is not None else indexing.OperatorIndexers()
    insights = insights if insights is not None else references.Insights()
    identity = identity if identity is not None else peering.detect_own_id(manual=False)
    vault = vault if vault is not None else credentials.Vault()
    memo = memo if memo is not None else ephemera.Memo()
    memo = ephemera.AnyMemo(memo)
    event_queue: posting.K8sEventQueue = asyncio.Queue()
    signal_flag: aiotasks.Future = asyncio.Future()
    started_flag: asyncio.Event = asyncio.Event()
    operator_paused = aiotoggles.ToggleSet(any)
    inner_tasks: list[aiotasks.Task] = []
    authn_tasks: list[aiotasks.Task] = []
    outer_tasks: list[aiotasks.Task] = []

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
    indexers.ensure(registry._indexing.get_all_handlers())

    # Global credentials store for this operator, also for CRD-reading & peering mode detection.
    auth.vault_var.set(vault)

    # Special case: pass the settings container through the user-side handlers (no explicit args).
    # Toolkits have to keep the original operator context somehow, and the only way is contextvars.
    posting.settings_var.set(settings)

    # A few common background forever-running infrastructural tasks (irregular root tasks).
    inner_tasks.append(asyncio.create_task(
        name="stop-flag checker",
        coro=_stop_flag_checker(
            signal_flag=signal_flag,
            stop_flag=stop_flag)))
    inner_tasks.append(asyncio.create_task(
        name="ultimate termination",
        coro=_ultimate_termination(
            settings=settings,
            stop_flag=stop_flag)))
    outer_tasks.append(asyncio.create_task(
        name="startup/cleanup activities",
        coro=_startup_cleanup_activities(
            ready_flag=ready_flag,
            started_flag=started_flag,
            registry=registry,
            settings=settings,
            indices=indexers.indices,
            vault=vault,
            memo=memo)))  # to purge & finalize the caches in the end.

    # Kill all the daemons gracefully when the operator exits (so that they are not "hung").
    outer_tasks.append(aiotasks.create_guarded_task(
        name="daemon killer", flag=started_flag, logger=logger,
        coro=daemons.daemon_killer(
            settings=settings,
            memories=memories,
            operator_paused=operator_paused)))

    # Keeping the credentials fresh and valid via the authentication handlers on demand.
    authn_tasks.append(aiotasks.create_guarded_task(
        name="credentials retriever", flag=started_flag, logger=logger,
        coro=activities.authenticator(
            registry=registry,
            settings=settings,
            indices=indexers.indices,
            vault=vault,
            memo=memo)))

    # K8s-event posting. Events are queued in-memory and posted in the background.
    # NB: currently, it is a global task, but can be made per-resource or per-object.
    inner_tasks.append(aiotasks.create_guarded_task(
        name="poster of events", flag=started_flag, logger=logger,
        coro=posting.poster(
            settings=settings,
            backbone=insights.backbone,
            event_queue=event_queue)))

    # Liveness probing -- so that Kubernetes would know that the operator is alive.
    if liveness_endpoint:
        inner_tasks.append(aiotasks.create_guarded_task(
            name="health reporter", flag=started_flag, logger=logger,
            coro=probing.health_reporter(
                registry=registry,
                settings=settings,
                endpoint=liveness_endpoint,
                indices=indexers.indices,
                memo=memo)))

    # Admission webhooks run as either a server or a tunnel or a fixed config.
    # The webhook manager automatically adjusts the cluster configuration at runtime.
    container: aiovalues.Container[reviews.WebhookClientConfig] = aiovalues.Container()
    inner_tasks.append(aiotasks.create_guarded_task(
        name="admission insights chain", flag=started_flag, logger=logger,
        coro=aiobindings.condition_chain(
            source=insights.revised, target=container.changed)))
    inner_tasks.append(aiotasks.create_guarded_task(
        name="admission validating configuration manager", flag=started_flag, logger=logger,
        coro=admission.validating_configuration_manager(
            container=container, settings=settings, registry=registry, insights=insights)))
    inner_tasks.append(aiotasks.create_guarded_task(
        name="admission mutating configuration manager", flag=started_flag, logger=logger,
        coro=admission.mutating_configuration_manager(
            container=container, settings=settings, registry=registry, insights=insights)))
    inner_tasks.append(aiotasks.create_guarded_task(
        name="admission webhook server", flag=started_flag, logger=logger,
        coro=admission.admission_webhook_server(
            container=container, settings=settings, registry=registry, insights=insights,
            webhookfn=functools.partial(admission.serve_admission_request,
                                        settings=settings, registry=registry, insights=insights,
                                        memories=memories, memobase=memo,
                                        indices=indexers.indices))))

    # Permanent observation of what resource kinds and namespaces are available in the cluster.
    # Spawn and cancel dimensional tasks as they come and go; dimensions = resources x namespaces.
    inner_tasks.append(aiotasks.create_guarded_task(
        name="resource observer", flag=started_flag, logger=logger,
        coro=observation.resource_observer(
            insights=insights,
            registry=registry,
            settings=settings)))
    inner_tasks.append(aiotasks.create_guarded_task(
        name="namespace observer", flag=started_flag, logger=logger,
        coro=observation.namespace_observer(
            clusterwide=clusterwide,
            namespaces=namespaces,
            insights=insights,
            settings=settings)))

    # Explicit command is a hack for the CLI to run coroutines in an operator-like environment.
    # If not specified, then use the normal resource processing. It is not exposed publicly (yet).
    if _command is not None:
        inner_tasks.append(aiotasks.create_guarded_task(
            name="the command", flag=started_flag, logger=logger, finishable=True,
            coro=_command))
    else:
        inner_tasks.append(aiotasks.create_guarded_task(
            name="multidimensional multitasker", flag=started_flag, logger=logger,
            coro=orchestration.orchestrator(
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
                                            operator_paused=operator_paused,
                                            event_queue=event_queue))))

    # It is a list of one task now by default. But a legacy flat list of tasks can be requested
    # for backwards compatibility; the trade-off: losing the extra safety on termination.
    # TODO: give them better names.
    # TODO: maybe wrap the outer_tasks to one final task, too? (but return as [task] for back-compat)
    tasks: MutableSequence[aiotasks.Task] = []
    if flat:
        tasks = outer_tasks + authn_tasks + inner_tasks
    else:
        authn_tasks.append(aiotasks.create_guarded_task(
            name="inner task group", logger=logger, finishable=True,
            coro=run_group(inner_tasks, name='inner'),
        ))
        outer_tasks.append(aiotasks.create_guarded_task(
            name="client task group", logger=logger, finishable=True,
            coro=run_group(authn_tasks, name='client'),
        ))
        tasks.append(aiotasks.create_guarded_task(
            name="outer task group", logger=logger, finishable=True,
            coro=run_group(outer_tasks, name='outer'),
        ))

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


async def run_group(
        tasks: Collection[aiotasks.Task],
        *,
        name: str,
        return_when: Any = asyncio.FIRST_COMPLETED,
        timeout: float | None = None,
        cancel_interval: float | None = 10,
        exit_interval: float | None = None,
) -> None:
    """
    Orchestrate the tasks and terminate them gracefully when needed.

    The nested tasks are expected to run forever. Their number is limited.
    Once any of them exits, the whole group and all tasks of the group exit.

    The nested tasks, in turn, can spawn multiple sub-tasks of various purposes.
    They can be awaited, monitored, or fired-and-forgot.

    The group can be wrapped into its own task and be a part of a bigger group.
    When the nested group exits, the parent group begins its own termination.
    This way, we form task termination sequences. But all tasks start at once,
    unless there is explicit orchestration via the readiness/starting flags.
    """
    logger.info(f"🔥 entering {name!r}")
    # Run the infinite tasks until one of them fails/exits (they never exit normally).
    # If the operator is cancelled, propagate the cancellation to all the sub-tasks.
    # There is no graceful period: cancel as soon as possible, but allow them to finish.
    try:
        done, pending = await aiotasks.wait(tasks, timeout=timeout, return_when=return_when)
    except asyncio.CancelledError:
        logger.info(f"🔥 cancelling {name!r}")
        await aiotasks.stop(tasks, title=name, logger=logger, cancelled=True, interval=cancel_interval)
        logger.info(f"🔥 cancelled {name!r}")
        raise

    # If the operator is intact, but one of the root tasks has exited (successfully or not),
    # cancel all the remaining root tasks, and gracefully exit other spawned sub-tasks.
    logger.info(f"🔥 stopping {name!r}")
    if pending:
        cancelled, _ = await aiotasks.stop(pending, title=name, logger=logger, interval=exit_interval)
    else:
        cancelled = set()
    logger.info(f"🔥 stopped {name!r}")

    # If succeeded or if cancellation is silenced, re-raise from failed tasks (if any).
    await aiotasks.reraise(done | cancelled)


async def run_tasks(
        root_tasks: Collection[aiotasks.Task],
        *,
        ignored: Collection[aiotasks.Task] = frozenset(),
) -> None:
    """
    Orchestrate the operator tasks and terminate them gracefully when needed.

    The root tasks are expected to run forever. Their number is limited. Once
    any of them exits, the whole operator and all other root tasks should exit.

    The hung tasks are those that were spawned during the operator runtime,
    and were not canceled/exited on the root tasks termination. They are given
    some extra time to finish, after which they are forcefully terminated too.

    .. note::
        Due to implementation details, every task created after the operator's
        startup is deemed to be a task or a sub-task of the operator.
        In the end, all tasks are forcefully canceled. Even if those tasks were
        created by other means. There is no way to trace who spawned what.
        Only the tasks that existed before the operator startup are ignored
        (for example, those that spawned the operator itself).
    """

    try:
        await run_group(root_tasks, name="Root")
    except asyncio.CancelledError:
        hung_tasks = await aiotasks.all_tasks(ignored=ignored)
        await aiotasks.stop(hung_tasks, title="Hung", logger=logger, cancelled=True, interval=1)
        raise
    except Exception:
        # TODO: terminate properly! (but how did it terminate before?)
        raise

    # After the root tasks are all gone, cancel any spawned sub-tasks (e.g. handlers).
    # If the operator is cancelled, propagate the cancellation to all the sub-tasks.
    # TODO: an assumption! the loop is not fully ours! find a way to cancel only our spawned tasks.
    hung_tasks = await aiotasks.all_tasks(ignored=ignored)
    await run_group(hung_tasks, name="Hung", timeout=5, return_when=asyncio.ALL_COMPLETED, cancel_interval=1, exit_interval=1)


async def _stop_flag_checker(
        signal_flag: aiotasks.Future,
        stop_flag: aioadapters.Flag | None,
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
        flags.append(asyncio.create_task(aioadapters.wait_flag(stop_flag), name="stop-flag waiter"))

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
            logger.info(f"Signal {result.name!s} is received. Operator is stopping.")
        else:
            logger.info(f"Stop-flag is set to {result!r}. Operator is stopping.")


async def _ultimate_termination(
        *,
        settings: configuration.OperatorSettings,
        stop_flag: aioadapters.Flag | None,
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
        if not aioadapters.check_flag(stop_flag):
            if settings.process.ultimate_exiting_timeout is not None:
                loop = asyncio.get_running_loop()
                loop.call_later(settings.process.ultimate_exiting_timeout,
                                signal.pthread_kill, threading.get_ident(), signal.SIGKILL)


async def _startup_cleanup_activities(
        ready_flag: aioadapters.Flag | None,
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

    Beside calling the startup/cleanup handlers, it performs a few operator-wide
    cleanups too (those that cannot be handled by garbage collection).
    """
    logger.debug(f"Starting Kopf {versions.version or '(unknown version)'}.")

    # Execute the startup activity before any root task starts running (due to readiness flag).
    try:
        await activities.run_activity(
            lifecycle=lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            activity=causes.Activity.STARTUP,
            indices=indices,
            memo=memo,
        )
    except asyncio.CancelledError:
        logger.warning("Startup activity is only partially executed due to cancellation.")
        raise

    # Notify the caller that we are ready to be executed. This unfreezes all the root tasks.
    started_flag.set()
    await aioadapters.raise_flag(ready_flag)

    # Sleep forever, or until cancelled, which happens when the operator begins its shutdown.
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass

    # Execute the cleanup activity after all other root tasks are presumably done.
    try:
        await activities.run_activity(
            lifecycle=lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            activity=causes.Activity.CLEANUP,
            indices=indices,
            memo=memo,
        )
        await vault.close()
    except asyncio.CancelledError:
        logger.warning("Cleanup activity is only partially executed due to cancellation.")
        raise
