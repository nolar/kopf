"""
Conversion of low-level events to high-level causes, and handling them.

These functions are invoked from the queueing module `kopf.reactor.queueing`,
which are the actual event loop of the operator process.

The conversion of the low-level events to the high-level causes is done by
checking the object's state and comparing it to the preserved last-seen state.

The framework itself makes the necessary changes to the object, -- such as the
finalizers attachment, last-seen state updates, and handler status tracking, --
thus provoking the low-level watch-events and additional queueing calls.
But these internal changes are filtered out from the cause detection
and therefore do not trigger the user-defined handlers.
"""
import asyncio
import time
from typing import Collection, Optional, Tuple

from kopf.engines import loggers, posting
from kopf.reactor import causation, daemons, effects, handling, indexing, lifecycles, registries
from kopf.storage import finalizers, states
from kopf.structs import bodies, configuration, containers, diffs, ephemera, \
                         handlers as handlers_, patches, primitives, references


async def process_resource_event(
        lifecycle: lifecycles.LifeCycleFn,
        indexers: indexing.OperatorIndexers,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        memories: containers.ResourceMemories,
        memobase: ephemera.AnyMemo,
        resource: references.Resource,
        raw_event: bodies.RawEvent,
        event_queue: posting.K8sEventQueue,
        stream_pressure: Optional[asyncio.Event] = None,  # None for tests
        resource_indexed: Optional[primitives.Toggle] = None,  # None for tests & observation
        operator_indexed: Optional[primitives.ToggleSet] = None,  # None for tests & observation
) -> None:
    """
    Handle a single custom object low-level watch-event.

    Convert the low-level events, as provided by the watching/queueing tasks,
    to the high-level causes, and then call the cause-handling logic.
    """

    # Recall what is stored about that object. Share it in little portions with the consumers.
    # And immediately forget it if the object is deleted from the cluster (but keep in memory).
    raw_type, raw_body = raw_event['type'], raw_event['object']
    memory = await memories.recall(raw_body, noticed_by_listing=raw_type is None, memo=memobase)
    if memory.live_fresh_body is not None:
        memory.live_fresh_body._replace_with(raw_body)
    if raw_type == 'DELETED':
        await memories.forget(raw_body)

    # Convert to a heavy mapping-view wrapper only now, when heavy processing begins.
    # Raw-event streaming, queueing, and batching use regular lightweight dicts.
    # Why here? 1. Before it splits into multiple causes & handlers for the same object's body;
    # 2. After it is batched (queueing); 3. While the "raw" parsed JSON is still known;
    # 4. Same as where a patch object of a similar wrapping semantics is created.
    body = memory.live_fresh_body if memory.live_fresh_body is not None else bodies.Body(raw_body)
    patch = patches.Patch()

    # Throttle the non-handler-related errors. The regular event watching/batching continues
    # to prevent queue overfilling, but the processing is skipped (events are ignored).
    # Choice of place: late enough to have a per-resource memory for a throttler; also, a logger.
    # But early enough to catch environment errors from K8s API, and from most of the complex code.
    async with effects.throttled(
        throttler=memory.error_throttler,
        logger=loggers.LocalObjectLogger(body=body, settings=settings),
        delays=settings.batching.error_delays,
        wakeup=stream_pressure,
    ) as should_run:
        if should_run:

            # Each object has its own prefixed logger, to distinguish parallel handling.
            logger = loggers.ObjectLogger(body=body, settings=settings)
            posting.event_queue_loop_var.set(asyncio.get_running_loop())
            posting.event_queue_var.set(event_queue)  # till the end of this object's task.

            # [Pre-]populate the indices. This must be lightweight.
            await indexing.index_resource(
                registry=registry,
                indexers=indexers,
                settings=settings,
                resource=resource,
                raw_event=raw_event,
                body=body,
                memory=memory,
                logger=loggers.TerseObjectLogger(body=body, settings=settings),
            )

            # Wait for all other individual resources and all other resource kinds' lists to finish.
            # If this one has changed while waiting for the global readiness, let it be reprocessed.
            if operator_indexed is not None and resource_indexed is not None:
                await operator_indexed.drop_toggle(resource_indexed)
            if operator_indexed is not None:
                await operator_indexed.wait_for(True)  # other resource kinds & objects.
            if stream_pressure is not None and stream_pressure.is_set():
                return

            # Do the magic -- do the job.
            delays, matched = await process_resource_causes(
                lifecycle=lifecycle,
                indexers=indexers,
                registry=registry,
                settings=settings,
                resource=resource,
                raw_event=raw_event,
                body=body,
                patch=patch,
                memory=memory,
                logger=logger,
            )

            # Whatever was done, apply the accumulated changes to the object, or sleep-n-touch for delays.
            # But only once, to reduce the number of API calls and the generated irrelevant events.
            # And only if the object is at least supposed to exist (not "GONE"), even if actually does not.
            if raw_event['type'] != 'DELETED':
                applied = await effects.apply(
                    settings=settings,
                    resource=resource,
                    body=body,
                    patch=patch,
                    logger=logger,
                    delays=delays,
                    stream_pressure=stream_pressure,
                )
                if applied and matched:
                    logger.debug(f"Handling cycle is finished, waiting for new changes since now.")


async def process_resource_causes(
        lifecycle: lifecycles.LifeCycleFn,
        indexers: indexing.OperatorIndexers,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        raw_event: bodies.RawEvent,
        body: bodies.Body,
        patch: patches.Patch,
        logger: loggers.ObjectLogger,
        memory: containers.ResourceMemory,
) -> Tuple[Collection[float], bool]:

    finalizer = settings.persistence.finalizer
    extra_fields = (
        # NB: indexing handlers are useless here, they are handled on their own.
        registry._resource_watching.get_extra_fields(resource=resource) |
        registry._resource_changing.get_extra_fields(resource=resource) |
        registry._resource_spawning.get_extra_fields(resource=resource))
    old = settings.persistence.diffbase_storage.fetch(body=body)
    new = settings.persistence.diffbase_storage.build(body=body, extra_fields=extra_fields)
    old = settings.persistence.progress_storage.clear(essence=old) if old is not None else None
    new = settings.persistence.progress_storage.clear(essence=new) if new is not None else None
    diff = diffs.diff(old, new)

    # Detect what are we going to do on this processing cycle.
    resource_watching_cause = causation.detect_resource_watching_cause(
        raw_event=raw_event,
        resource=resource,
        indices=indexers.indices,
        logger=logger,
        patch=patch,
        body=body,
        memo=memory.memo,
    ) if registry._resource_watching.has_handlers(resource=resource) else None

    resource_spawning_cause = causation.detect_resource_spawning_cause(
        resource=resource,
        indices=indexers.indices,
        logger=logger,
        patch=patch,
        body=body,
        memo=memory.memo,
        reset=bool(diff),  # only essential changes reset idling, not every event
    ) if registry._resource_spawning.has_handlers(resource=resource) else None

    resource_changing_cause = causation.detect_resource_changing_cause(
        finalizer=finalizer,
        raw_event=raw_event,
        resource=resource,
        indices=indexers.indices,
        logger=logger,
        patch=patch,
        body=body,
        old=old,
        new=new,
        diff=diff,
        memo=memory.memo,
        initial=memory.noticed_by_listing and not memory.fully_handled_once,
    ) if registry._resource_changing.has_handlers(resource=resource) else None

    # If there are any handlers for this resource kind in general, but not for this specific object
    # due to filters, then be blind to it, store no state, and log nothing about the handling cycle.
    if (resource_changing_cause is not None and
        not registry._resource_changing.prematch(cause=resource_changing_cause)):
        resource_changing_cause = None

    # Block the object from deletion if we have anything to do in its end of life:
    # specifically, if there are daemons to kill or mandatory on-deletion handlers to call.
    # The high-level handlers are prevented if this event cycle is dedicated to the finalizer.
    # The low-level handlers (on-event spying & daemon spawning) are still executed asap.
    deletion_is_ongoing = finalizers.is_deletion_ongoing(body=body)
    deletion_is_blocked = finalizers.is_deletion_blocked(body=body, finalizer=finalizer)
    deletion_must_be_blocked = (
        (resource_spawning_cause is not None and
         registry._resource_spawning.requires_finalizer(
             cause=resource_spawning_cause,
             excluded=memory.forever_stopped,
         ))
        or
        (resource_changing_cause is not None and
         registry._resource_changing.requires_finalizer(
             cause=resource_changing_cause,
         )))

    if deletion_must_be_blocked and not deletion_is_blocked and not deletion_is_ongoing:
        logger.debug("Adding the finalizer, thus preventing the actual deletion.")
        finalizers.block_deletion(body=body, patch=patch, finalizer=finalizer)
        resource_changing_cause = None  # prevent further high-level processing this time

    if not deletion_must_be_blocked and deletion_is_blocked:
        logger.debug("Removing the finalizer, as there are no handlers requiring it.")
        finalizers.allow_deletion(body=body, patch=patch, finalizer=finalizer)
        resource_changing_cause = None  # prevent further high-level processing this time

    # Invoke all the handlers that should or could be invoked at this processing cycle.
    # The low-level spies go ASAP always. However, the daemons are spawned before the high-level
    # handlers and killed after them: the daemons should live throughout the full object lifecycle.
    if resource_watching_cause is not None:
        await process_resource_watching_cause(
            lifecycle=lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            cause=resource_watching_cause,
        )

    resource_spawning_delays: Collection[float] = []
    if resource_spawning_cause is not None:
        resource_spawning_delays = await process_resource_spawning_cause(
            registry=registry,
            settings=settings,
            memory=memory,
            cause=resource_spawning_cause,
        )

    resource_changing_delays: Collection[float] = []
    if resource_changing_cause is not None:
        resource_changing_delays = await process_resource_changing_cause(
            lifecycle=lifecycle,
            registry=registry,
            settings=settings,
            memory=memory,
            cause=resource_changing_cause,
        )

    # Release the object if everything is done, and it is marked for deletion.
    # But not when it has already gone.
    if deletion_is_ongoing and deletion_is_blocked \
            and not resource_spawning_delays \
            and not resource_changing_delays:
        logger.debug("Removing the finalizer, thus allowing the actual deletion.")
        finalizers.allow_deletion(body=body, patch=patch, finalizer=finalizer)

    delays = list(resource_spawning_delays) + list(resource_changing_delays)
    return (delays, resource_changing_cause is not None)


async def process_resource_watching_cause(
        lifecycle: lifecycles.LifeCycleFn,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        cause: causation.ResourceWatchingCause,
) -> None:
    """
    Handle a received event, log but ignore all errors.

    This is a lightweight version of the cause handling, but for the raw events,
    without any progress persistence. Multi-step calls are also not supported.
    If the handler fails, it fails and is never retried.

    Note: K8s-event posting is skipped for `kopf.on.event` handlers,
    as they should be silent. Still, the messages are logged normally.
    """
    handlers = registry._resource_watching.get_handlers(cause=cause)
    outcomes = await handling.execute_handlers_once(
        lifecycle=lifecycle,
        settings=settings,
        handlers=handlers,
        cause=cause,
        state=states.State.from_scratch().with_handlers(handlers),
        default_errors=handlers_.ErrorsMode.IGNORED,
    )

    # Store the results, but not the handlers' progress.
    states.deliver_results(outcomes=outcomes, patch=cause.patch)


async def process_resource_spawning_cause(
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        memory: containers.ResourceMemory,
        cause: causation.ResourceSpawningCause,
) -> Collection[float]:
    """
    Spawn/kill all the background tasks of a resource.

    The spawning and killing happens in parallel with the resource-changing
    handlers invocation (even if it takes few cycles). For this, the signal
    to terminate is sent to the daemons immediately, but the actual check
    of their shutdown is performed only when all the on-deletion handlers
    have succeeded (or after they were invoked if they are optional;
    or immediately if there were no on-deletion handlers to invoke at all).

    The resource remains blocked by the finalizers until all the daemons exit
    (except those marked as tolerating being orphaned).
    """

    # Refresh the up-to-date body & essential timestamp for all the daemons/timers.
    if memory.live_fresh_body is None:
        memory.live_fresh_body = cause.body
    if cause.reset:
        memory.idle_reset_time = time.monotonic()

    if finalizers.is_deletion_ongoing(cause.body):
        stopping_delays = await daemons.stop_resource_daemons(
            settings=settings,
            daemons=memory.running_daemons,
        )
        return stopping_delays

    else:
        handlers = registry._resource_spawning.get_handlers(
            cause=cause,
            excluded=memory.forever_stopped,
        )
        spawning_delays = await daemons.spawn_resource_daemons(
            settings=settings,
            daemons=memory.running_daemons,
            cause=cause,
            memory=memory,
            handlers=handlers,
        )
        matching_delays = await daemons.match_resource_daemons(
            settings=settings,
            daemons=memory.running_daemons,
            handlers=handlers,
        )
        return list(spawning_delays) + list(matching_delays)


async def process_resource_changing_cause(
        lifecycle: lifecycles.LifeCycleFn,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        memory: containers.ResourceMemory,
        cause: causation.ResourceChangingCause,
) -> Collection[float]:
    """
    Handle a detected cause, as part of the bigger handler routine.
    """
    logger = cause.logger
    patch = cause.patch  # TODO get rid of this alias
    body = cause.body  # TODO get rid of this alias
    delays: Collection[float] = []
    done: Optional[bool] = None
    skip: Optional[bool] = None

    # Regular causes invoke the handlers.
    if cause.reason in handlers_.HANDLER_REASONS:
        title = handlers_.TITLES.get(cause.reason, repr(cause.reason))

        resource_registry = registry._resource_changing
        owned_handlers = resource_registry.get_resource_handlers(resource=cause.resource)
        cause_handlers = resource_registry.get_handlers(cause=cause)
        storage = settings.persistence.progress_storage
        state = states.State.from_storage(body=cause.body, storage=storage, handlers=owned_handlers)
        state = state.with_purpose(cause.reason).with_handlers(cause_handlers)

        # Report the causes that have been superseded (intercepted, overridden) by the current one.
        # The mix-in causes (i.e. resuming) is re-purposed if its handlers are still selected.
        # To the next cycle, all extras are purged or re-purposed, so the message does not repeat.
        for extra_reason, counters in state.extras.items():  # usually 0..1 items, rarely 2+.
            extra_title = handlers_.TITLES.get(extra_reason, repr(extra_reason))
            logger.info(f"{extra_title.capitalize()} is superseded by {title.lower()}: "
                        f"{counters.success} succeeded; "
                        f"{counters.failure} failed; "
                        f"{counters.running} left to the moment.")
            state = state.with_purpose(purpose=cause.reason, handlers=cause_handlers)

        # Purge the now-irrelevant handlers if they were not re-purposed (extras are recalculated!).
        # The current cause continues afterwards, and overrides its own pre-purged handler states.
        # TODO: purge only the handlers that fell out of current purpose; but it is not critical
        if state.extras:
            state.purge(body=cause.body, patch=cause.patch,
                        storage=storage, handlers=owned_handlers)

        # Inform on the current cause/event on every processing cycle. Even if there are
        # no handlers -- to show what has happened and why the diff-base is patched.
        logger.debug(f"{title.capitalize()} is in progress: %r", body)
        if cause.diff and cause.old is not None and cause.new is not None:
            logger.debug(f"{title.capitalize()} diff: %r", cause.diff)

        if cause_handlers:
            outcomes = await handling.execute_handlers_once(
                lifecycle=lifecycle,
                settings=settings,
                handlers=cause_handlers,
                cause=cause,
                state=state,
            )
            state = state.with_outcomes(outcomes)
            state.store(body=cause.body, patch=cause.patch, storage=storage)
            states.deliver_results(outcomes=outcomes, patch=cause.patch)

            if state.done:
                counters = state.counts  # calculate only once
                logger.info(f"{title.capitalize()} is processed: "
                            f"{counters.success} succeeded; "
                            f"{counters.failure} failed.")
                state.purge(body=cause.body, patch=cause.patch,
                            storage=storage, handlers=owned_handlers)

            done = state.done
            delays = state.delays
        else:
            skip = True

    # Regular causes also do some implicit post-handling when all handlers are done.
    if done or skip:
        if cause.new is not None and cause.old != cause.new:
            settings.persistence.diffbase_storage.store(body=body, patch=patch, essence=cause.new)

        # Once all handlers have succeeded at least once for any reason, or if there were none,
        # prevent further resume-handlers (which otherwise happens on each watch-stream re-listing).
        memory.fully_handled_once = True

    # Informational causes just print the log lines.
    if cause.reason == handlers_.Reason.GONE:
        logger.debug("Deleted, really deleted, and we are notified.")

    if cause.reason == handlers_.Reason.FREE:
        logger.debug("Deletion, but we are done with it, and we do not care.")

    if cause.reason == handlers_.Reason.NOOP:
        logger.debug("Something has changed, but we are not interested (the essence is the same).")

    # The delay is then consumed by the main handling routine (in different ways).
    return delays
