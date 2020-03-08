import asyncio
import functools
import logging
import signal
import threading
import warnings
from typing import (Optional, Collection, Tuple, Set, Any, Coroutine,
                    Sequence, MutableSequence, cast, TYPE_CHECKING)

from kopf.clients import auth
from kopf.engines import peering
from kopf.engines import posting
from kopf.engines import probing
from kopf.reactor import activities
from kopf.reactor import daemons
from kopf.reactor import lifecycles
from kopf.reactor import processing
from kopf.reactor import queueing
from kopf.reactor import registries
from kopf.structs import configuration
from kopf.structs import containers
from kopf.structs import credentials
from kopf.structs import handlers
from kopf.structs import primitives

if TYPE_CHECKING:
    asyncio_Task = asyncio.Task[None]
    asyncio_Future = asyncio.Future[Any]
else:
    asyncio_Task = asyncio.Task
    asyncio_Future = asyncio.Future

Tasks = Collection[asyncio_Task]

logger = logging.getLogger(__name__)

# An exchange point between login() and run()/operator().
# DEPRECATED: As soon as login() is removed, this global variable is not needed.
global_vault: Optional[credentials.Vault] = None


def login(
        verify: bool = False,  # DEPRECATED: kept for backward compatibility
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
) -> None:
    """
    Login to Kubernetes cluster, locally or remotely.

    Keep the logged in state or config object in the global variable,
    so that it is available for future operator runs.
    """
    warnings.warn("kopf.login() is deprecated; the operator now authenticates "
                  "internally; cease using kopf.login().", DeprecationWarning)

    # Remember the credentials store for later usage in the actual operator.
    # Set the global vault for the legacy login()->run() scenario.
    global global_vault
    global_vault = credentials.Vault()

    # Perform the initial one-time authentication in presumably the same loop.
    loop = loop if loop is not None else asyncio.get_event_loop()
    registry = registries.get_default_registry()
    settings = configuration.OperatorSettings()
    try:
        loop.run_until_complete(activities.authenticate(
            registry=registry,
            settings=settings,
            vault=global_vault,
        ))
    except asyncio.CancelledError:
        pass
    except activities.ActivityError as e:
        # Detect and re-raise the original LoginErrors, not the general activity error.
        # This is only needed for the legacy one-shot login, not for a background job.
        for outcome in e.outcomes.values():
            if isinstance(outcome.exception, credentials.LoginError):
                raise outcome.exception
        raise


def run(
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        lifecycle: Optional[lifecycles.LifeCycleFn] = None,
        registry: Optional[registries.OperatorRegistry] = None,
        settings: Optional[configuration.OperatorSettings] = None,
        memories: Optional[containers.ResourceMemories] = None,
        standalone: bool = False,
        priority: int = 0,
        peering_name: Optional[str] = None,
        liveness_endpoint: Optional[str] = None,
        namespace: Optional[str] = None,
        stop_flag: Optional[primitives.Flag] = None,
        ready_flag: Optional[primitives.Flag] = None,
        vault: Optional[credentials.Vault] = None,
) -> None:
    """
    Run the whole operator synchronously.

    This function should be used to run an operator in normal sync mode.
    """
    loop = loop if loop is not None else asyncio.get_event_loop()
    try:
        loop.run_until_complete(operator(
            lifecycle=lifecycle,
            registry=registry,
            settings=settings,
            memories=memories,
            standalone=standalone,
            namespace=namespace,
            priority=priority,
            peering_name=peering_name,
            liveness_endpoint=liveness_endpoint,
            stop_flag=stop_flag,
            ready_flag=ready_flag,
            vault=vault,
        ))
    except asyncio.CancelledError:
        pass


async def operator(
        *,
        lifecycle: Optional[lifecycles.LifeCycleFn] = None,
        registry: Optional[registries.OperatorRegistry] = None,
        settings: Optional[configuration.OperatorSettings] = None,
        memories: Optional[containers.ResourceMemories] = None,
        standalone: bool = False,
        priority: int = 0,
        peering_name: Optional[str] = None,
        liveness_endpoint: Optional[str] = None,
        namespace: Optional[str] = None,
        stop_flag: Optional[primitives.Flag] = None,
        ready_flag: Optional[primitives.Flag] = None,
        vault: Optional[credentials.Vault] = None,
) -> None:
    """
    Run the whole operator asynchronously.

    This function should be used to run an operator in an asyncio event-loop
    if the operator is orchestrated explicitly and manually.

    It is efficiently `spawn_tasks` + `run_tasks` with some safety.
    """
    existing_tasks = await _all_tasks()
    operator_tasks = await spawn_tasks(
        lifecycle=lifecycle,
        registry=registry,
        settings=settings,
        memories=memories,
        standalone=standalone,
        namespace=namespace,
        priority=priority,
        peering_name=peering_name,
        liveness_endpoint=liveness_endpoint,
        stop_flag=stop_flag,
        ready_flag=ready_flag,
        vault=vault,
    )
    await run_tasks(operator_tasks, ignored=existing_tasks)


async def spawn_tasks(
        *,
        lifecycle: Optional[lifecycles.LifeCycleFn] = None,
        registry: Optional[registries.OperatorRegistry] = None,
        settings: Optional[configuration.OperatorSettings] = None,
        memories: Optional[containers.ResourceMemories] = None,
        standalone: bool = False,
        priority: int = 0,
        peering_name: Optional[str] = None,
        liveness_endpoint: Optional[str] = None,
        namespace: Optional[str] = None,
        stop_flag: Optional[primitives.Flag] = None,
        ready_flag: Optional[primitives.Flag] = None,
        vault: Optional[credentials.Vault] = None,
) -> Tasks:
    """
    Spawn all the tasks needed to run the operator.

    The tasks are properly inter-connected with the synchronisation primitives.
    """
    loop = asyncio.get_running_loop()

    # The freezer and the registry are scoped to this whole task-set, to sync them all.
    lifecycle = lifecycle if lifecycle is not None else lifecycles.get_default_lifecycle()
    registry = registry if registry is not None else registries.get_default_registry()
    settings = settings if settings is not None else configuration.OperatorSettings()
    memories = memories if memories is not None else containers.ResourceMemories()
    vault = vault if vault is not None else global_vault
    vault = vault if vault is not None else credentials.Vault()
    event_queue: posting.K8sEventQueue = asyncio.Queue()
    freeze_mode: primitives.Toggle = primitives.Toggle()
    signal_flag: asyncio_Future = asyncio.Future()
    ready_flag = ready_flag if ready_flag is not None else asyncio.Event()
    tasks: MutableSequence[asyncio_Task] = []

    # Global credentials store for this operator, also for CRD-reading & peering mode detection.
    auth.vault_var.set(vault)

    # Special case: pass the settings container through the user-side handlers (no explicit args).
    # Toolkits have to keep the original operator context somehow, and the only way is contextvars.
    posting.settings_var.set(settings)

    # Few common background forever-running infrastructural tasks (irregular root tasks).
    tasks.extend([
        loop.create_task(_stop_flag_checker(
            signal_flag=signal_flag,
            stop_flag=stop_flag,
        )),
        loop.create_task(_startup_cleanup_activities(
            root_tasks=tasks,  # used as a "live" view, populated later.
            ready_flag=ready_flag,
            registry=registry,
            settings=settings,
            vault=vault,  # to purge & finalize the caches in the end.
        )),
    ])

    # Kill all the daemons gracefully when the operator exits (so that they are not "hung").
    tasks.extend([
        loop.create_task(_root_task_checker(
            name="daemon killer", ready_flag=ready_flag,
            coro=daemons.daemon_killer(
                settings=settings,
                memories=memories))),
    ])

    # Keeping the credentials fresh and valid via the authentication handlers on demand.
    tasks.extend([
        loop.create_task(_root_task_checker(
            name="credentials retriever", ready_flag=ready_flag,
            coro=activities.authenticator(
                registry=registry,
                settings=settings,
                vault=vault))),
    ])

    # K8s-event posting. Events are queued in-memory and posted in the background.
    # NB: currently, it is a global task, but can be made per-resource or per-object.
    tasks.extend([
        loop.create_task(_root_task_checker(
            name="poster of events", ready_flag=ready_flag,
            coro=posting.poster(
                event_queue=event_queue))),
    ])

    # Liveness probing -- so that Kubernetes would know that the operator is alive.
    if liveness_endpoint:
        tasks.extend([
            loop.create_task(_root_task_checker(
                name="health reporter", ready_flag=ready_flag,
                coro=probing.health_reporter(
                    registry=registry,
                    settings=settings,
                    endpoint=liveness_endpoint))),
        ])

    # Monitor the peers, unless explicitly disabled.
    ourselves: Optional[peering.Peer] = await peering.Peer.detect(
        id=peering.detect_own_id(), priority=priority,
        standalone=standalone, namespace=namespace, name=peering_name,
    )
    if ourselves:
        tasks.extend([
            loop.create_task(peering.peers_keepalive(
                ourselves=ourselves)),
            loop.create_task(_root_task_checker(
                name="watcher of peering", ready_flag=ready_flag,
                coro=queueing.watcher(
                    namespace=namespace,
                    settings=settings,
                    resource=ourselves.resource,
                    processor=functools.partial(peering.process_peering_event,
                                                ourselves=ourselves,
                                                freeze_mode=freeze_mode)))),
        ])

    # Resource event handling, only once for every known resource (de-duplicated).
    for resource in registry.resources:
        tasks.extend([
            loop.create_task(_root_task_checker(
                name=f"watcher of {resource.name}", ready_flag=ready_flag,
                coro=queueing.watcher(
                    namespace=namespace,
                    settings=settings,
                    resource=resource,
                    freeze_mode=freeze_mode,
                    processor=functools.partial(processing.process_resource_event,
                                                lifecycle=lifecycle,
                                                registry=registry,
                                                settings=settings,
                                                memories=memories,
                                                resource=resource,
                                                event_queue=event_queue)))),
        ])

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
        root_tasks: Tasks,
        *,
        ignored: Tasks = frozenset(),
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
        root_done, root_pending = await _wait(root_tasks, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        await _stop(root_tasks, title="Root", cancelled=True, interval=10)
        hung_tasks = await _all_tasks(ignored=ignored)
        await _stop(hung_tasks, title="Hung", cancelled=True, interval=1)
        raise

    # If the operator is intact, but one of the root tasks has exited (successfully or not),
    # cancel all the remaining root tasks, and gracefully exit other spawned sub-tasks.
    root_cancelled, _ = await _stop(root_pending, title="Root", cancelled=False)

    # After the root tasks are all gone, cancel any spawned sub-tasks (e.g. handlers).
    # If the operator is cancelled, propagate the cancellation to all the sub-tasks.
    # TODO: an assumption! the loop is not fully ours! find a way to cancel only our spawned tasks.
    hung_tasks = await _all_tasks(ignored=ignored)
    try:
        hung_done, hung_pending = await _wait(hung_tasks, timeout=5)
    except asyncio.CancelledError:
        await _stop(hung_tasks, title="Hung", cancelled=True, interval=1)
        raise

    # If the operator is intact, but the timeout is reached, forcely cancel the sub-tasks.
    hung_cancelled, _ = await _stop(hung_pending, title="Hung", cancelled=False, interval=1)

    # If succeeded or if cancellation is silenced, re-raise from failed tasks (if any).
    await _reraise(root_done | root_cancelled | hung_done | hung_cancelled)


async def _all_tasks(
        ignored: Tasks = frozenset(),
) -> Tasks:
    current_task = asyncio.current_task()
    return {task for task in asyncio.all_tasks()
            if task is not current_task and task not in ignored}


async def _wait(
        tasks: Tasks,
        *,
        timeout: Optional[float] = None,
        return_when: Any = asyncio.ALL_COMPLETED,
) -> Tuple[Set[asyncio_Task], Set[asyncio_Task]]:
    if not tasks:
        return set(), set()
    done, pending = await asyncio.wait(tasks, timeout=timeout, return_when=return_when)
    return cast(Set[asyncio_Task], done), cast(Set[asyncio_Task], pending)


async def _stop(
        tasks: Tasks,
        title: str,
        cancelled: bool,
        interval: Optional[float] = None,
) -> Tuple[Set[asyncio_Task], Set[asyncio_Task]]:
    if not tasks:
        logger.debug(f"{title} tasks stopping is skipped: no tasks given.")
        return set(), set()

    for task in tasks:
        task.cancel()

    done_ever: Set[asyncio_Task] = set()
    pending: Set[asyncio_Task] = set(tasks)
    while pending:
        # If the waiting (current) task is cancelled before the wait is over,
        # propagate the cancellation to all the awaited (sub-) tasks, and let them finish.
        try:
            done_now, pending = await _wait(pending, timeout=interval)
        except asyncio.CancelledError:
            # If the waiting (current) task is cancelled while propagating the cancellation
            # (i.e. double-cancelled), let it fail without graceful cleanup. It is urgent, it seems.
            pending = {task for task in tasks if not task.done()}
            are = 'are' if not pending else 'are not'
            why = 'double-cancelling at stopping' if cancelled else 'cancelling at stopping'
            logger.debug(f"{title} tasks {are} stopped: {why}; tasks left: {pending!r}")
            raise  # the repeated cancellation, handled specially.
        else:
            # If the cancellation is propagated normally and the awaited (sub-) tasks exited,
            # consider it as a successful cleanup.
            are = 'are' if not pending else 'are not'
            why = 'cancelling normally' if cancelled else 'finishing normally'
            logger.debug(f"{title} tasks {are} stopped: {why}; tasks left: {pending!r}")
            done_ever |= done_now

    return done_ever, pending


async def _reraise(
        tasks: Tasks,
) -> None:
    for task in tasks:
        try:
            task.result()  # can raise the regular (non-cancellation) exceptions.
        except asyncio.CancelledError:
            pass


async def _root_task_checker(
        name: str,
        ready_flag: primitives.Flag,
        coro: Coroutine[Any, Any, Any],
) -> None:

    # Wait until the startup activity succeeds. The wait will be cancelled if the startup failed.
    await primitives.wait_flag(ready_flag)

    # Actually run the root task, and log the outcome.
    try:
        await coro
    except asyncio.CancelledError:
        logger.debug(f"Root task {name!r} is cancelled.")
        raise
    except Exception as e:
        logger.exception(f"Root task {name!r} is failed: %s", e)
        raise  # fail the process and its exit status
    else:
        logger.warning(f"Root task {name!r} is finished unexpectedly.")


async def _stop_flag_checker(
        signal_flag: asyncio_Future,
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
        flags.append(asyncio.create_task(primitives.wait_flag(stop_flag)))

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


async def _startup_cleanup_activities(
        root_tasks: Sequence[asyncio_Task],  # mutated externally!
        ready_flag: Optional[primitives.Flag],
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        vault: credentials.Vault,
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
        )
    except asyncio.CancelledError:
        logger.warning("Startup activity is only partially executed due to cancellation.")
        raise

    # Notify the caller that we are ready to be executed. This unfreezes all the root tasks.
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
        await _wait(awaited_tasks)
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
        )
        await vault.close()
    except asyncio.CancelledError:
        logger.warning("Cleanup activity is only partially executed due to cancellation.")
        raise


def create_tasks(
        loop: asyncio.AbstractEventLoop,
        *arg: Any,
        **kwargs: Any,
) -> Tasks:
    """
    .. deprecated:: 1.0
        This is a synchronous interface to `spawn_tasks`.
        It is only kept for backward compatibility, as it was exposed
        via the public interface of the framework.
    """
    warnings.warn("kopf.create_tasks() is deprecated: "
                  "use kopf.spawn_tasks() or kopf.operator().",
                  DeprecationWarning)
    return loop.run_until_complete(spawn_tasks(*arg, **kwargs))
