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
import datetime
from typing import Collection, Optional

from kopf.clients import patching
from kopf.engines import logging as logging_engine
from kopf.engines import posting
from kopf.engines import sleeping
from kopf.reactor import causation
from kopf.reactor import handling
from kopf.reactor import lifecycles
from kopf.reactor import registries
from kopf.storage import finalizers
from kopf.storage import states
from kopf.structs import bodies
from kopf.structs import configuration
from kopf.structs import containers
from kopf.structs import diffs
from kopf.structs import handlers as handlers_
from kopf.structs import patches
from kopf.structs import resources

# How often to wake up from the long sleep, to show liveness in the logs.
WAITING_KEEPALIVE_INTERVAL = 10 * 60


async def process_resource_event(
        lifecycle: lifecycles.LifeCycleFn,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        memories: containers.ResourceMemories,
        resource: resources.Resource,
        raw_event: bodies.RawEvent,
        replenished: asyncio.Event,
        event_queue: posting.K8sEventQueue,
) -> None:
    """
    Handle a single custom object low-level watch-event.

    Convert the low-level events, as provided by the watching/queueing tasks,
    to the high-level causes, and then call the cause-handling logic.

    All the internally provoked changes are intercepted, do not create causes,
    and therefore do not call the handling logic.
    """

    # Convert to a heavy mapping-view wrapper only now, when heavy processing begins.
    # Raw-event streaming, queueing, and batching use regular lightweight dicts.
    # Why here? 1. Before it splits into multiple causes & handlers for the same object's body;
    # 2. After it is batched (queueing); 3. While the "raw" parsed JSON is still known;
    # 4. Same as where a patch object of a similar wrapping semantics is created.
    body = bodies.Body(raw_event['object'])
    patch = patches.Patch()
    delay: Optional[float] = None

    # Each object has its own prefixed logger, to distinguish parallel handling.
    logger = logging_engine.ObjectLogger(body=body, settings=settings)
    posting.event_queue_loop_var.set(asyncio.get_running_loop())
    posting.event_queue_var.set(event_queue)  # till the end of this object's task.

    # Recall what is stored about that object. Share it in little portions with the consumers.
    # And immediately forget it if the object is deleted from the cluster (but keep in memory).
    memory = await memories.recall(body, noticed_by_listing=raw_event['type'] is None)
    if raw_event['type'] == 'DELETED':
        await memories.forget(body)

    extra_fields = registry.resource_changing_handlers[resource].get_extra_fields()
    old = settings.persistence.diffbase_storage.fetch(body=body)
    new = settings.persistence.diffbase_storage.build(body=body, extra_fields=extra_fields)
    old = settings.persistence.progress_storage.clear(essence=old) if old is not None else None
    new = settings.persistence.progress_storage.clear(essence=new) if new is not None else None
    diff = diffs.diff(old, new)

    # Detect what are we going to do on this processing cycle.
    resource_watching_cause = causation.detect_resource_watching_cause(
        raw_event=raw_event,
        resource=resource,
        logger=logger,
        patch=patch,
        body=body,
        memo=memory.user_data,
    ) if registry.resource_watching_handlers[resource] else None

    resource_changing_cause = causation.detect_resource_changing_cause(
        raw_event=raw_event,
        resource=resource,
        logger=logger,
        patch=patch,
        body=body,
        old=old,
        new=new,
        diff=diff,
        memo=memory.user_data,
        initial=memory.noticed_by_listing and not memory.fully_handled_once,
    ) if registry.resource_changing_handlers[resource] else None

    # Invoke all the handlers that should or could be invoked at this processing cycle.
    if resource_watching_cause is not None:
        await process_resource_watching_cause(
            lifecycle=lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            cause=resource_watching_cause,
        )

    # Object patch accumulator. Populated by the methods. Applied in the end of the handler.
    # Detect the cause and handle it (or at least log this happened).
    resource_changing_delays: Collection[float] = []
    if resource_changing_cause is not None:
        resource_changing_delays = await process_resource_changing_cause(
            lifecycle=lifecycle,
            registry=registry,
            settings=settings,
            memory=memory,
            cause=resource_changing_cause,
        )

    # Whatever was done, apply the accumulated changes to the object, or sleep-n-touch for delays.
    # But only once, to reduce the number of API calls and the generated irrelevant events.
    # And only of the object is at least supposed to exist (not "GONE"), even if actually does not.
    if raw_event['type'] != 'DELETED':
        await apply_reaction_outcomes(
            resource=resource, body=body,
            patch=patch, delays=resource_changing_delays,
            logger=logger, replenished=replenished)


async def apply_reaction_outcomes(
        *,
        resource: resources.Resource,
        body: bodies.Body,
        patch: patches.Patch,
        delays: Collection[float],
        logger: logging_engine.ObjectLogger,
        replenished: asyncio.Event,
) -> None:
    delay = min(delays) if delays else None

    if patch:
        logger.debug("Patching with: %r", patch)
        await patching.patch_obj(resource=resource, patch=patch, body=body)

    # Sleep strictly after patching, never before -- to keep the status proper.
    # The patching above, if done, interrupts the sleep instantly, so we skip it at all.
    # Note: a zero-second or negative sleep is still a sleep, it will trigger a dummy patch.
    if delay and patch:
        logger.debug(f"Sleeping was skipped because of the patch, {delay} seconds left.")
    elif delay is None and not patch:
        logger.debug(f"Handling cycle is finished, waiting for new changes since now.")
    elif delay is not None:
        if delay > WAITING_KEEPALIVE_INTERVAL:
            limit = WAITING_KEEPALIVE_INTERVAL
            logger.debug(f"Sleeping for {delay} (capped {limit}) seconds for the delayed handlers.")
            unslept_delay = await sleeping.sleep_or_wait(limit, replenished)
        elif delay > 0:
            logger.debug(f"Sleeping for {delay} seconds for the delayed handlers.")
            unslept_delay = await sleeping.sleep_or_wait(delay, replenished)
        else:
            unslept_delay = None  # no need to sleep? means: slept in full.

        if unslept_delay is not None:
            logger.debug(f"Sleeping was interrupted by new changes, {unslept_delay} seconds left.")
        else:
            # Any unique always-changing value will work; not necessary a timestamp.
            dummy_value = datetime.datetime.utcnow().isoformat()
            dummy_patch = patches.Patch({'status': {'kopf': {'dummy': dummy_value}}})
            logger.debug("Provoking reaction with: %r", dummy_patch)
            await patching.patch_obj(resource=resource, patch=dummy_patch, body=body)


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
    handlers = registry.resource_watching_handlers[cause.resource].get_handlers(cause=cause)
    outcomes = await handling.execute_handlers_once(
        lifecycle=lifecycle,
        settings=settings,
        handlers=handlers,
        cause=cause,
        state=states.State.from_scratch(handlers=handlers),
        default_errors=handlers_.ErrorsMode.IGNORED,
    )

    # Store the results, but not the handlers' progress.
    states.deliver_results(outcomes=outcomes, patch=cause.patch)


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

    resource_changing_handlers = registry.resource_changing_handlers[cause.resource]
    deletion_must_be_blocked = resource_changing_handlers.requires_finalizer(cause=cause)
    deletion_is_blocked = finalizers.is_deletion_blocked(body=cause.body)

    if deletion_must_be_blocked and not deletion_is_blocked:
        logger.debug("Adding the finalizer, thus preventing the actual deletion.")
        finalizers.block_deletion(body=body, patch=patch)
        return ()

    if not deletion_must_be_blocked and deletion_is_blocked:
        logger.debug("Removing the finalizer, as there are no handlers requiring it.")
        finalizers.allow_deletion(body=body, patch=patch)
        return ()

    # Regular causes invoke the handlers.
    if cause.reason in handlers_.HANDLER_REASONS:
        title = handlers_.TITLES.get(cause.reason, repr(cause.reason))
        logger.debug(f"{title.capitalize()} event: %r", body)
        if cause.diff and cause.old is not None and cause.new is not None:
            logger.debug(f"{title.capitalize()} diff: %r", cause.diff)

        handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause=cause)
        storage = settings.persistence.progress_storage
        state = states.State.from_storage(body=cause.body, storage=storage, handlers=handlers)
        if handlers:
            outcomes = await handling.execute_handlers_once(
                lifecycle=lifecycle,
                settings=settings,
                handlers=handlers,
                cause=cause,
                state=state,
            )
            state = state.with_outcomes(outcomes)
            state.store(body=cause.body, patch=cause.patch, storage=storage)
            states.deliver_results(outcomes=outcomes, patch=cause.patch)

            if state.done:
                logger.info(f"All handlers succeeded for {title}.")
                state.purge(body=cause.body, patch=cause.patch, storage=storage)

            done = state.done
            delays = state.delays
        else:
            skip = True

    # Regular causes also do some implicit post-handling when all handlers are done.
    if done or skip:
        if cause.new is not None and cause.old != cause.new:
            settings.persistence.diffbase_storage.store(body=body, patch=patch, essence=cause.new)
        if cause.reason == handlers_.Reason.DELETE:
            logger.debug("Removing the finalizer, thus allowing the actual deletion.")
            finalizers.allow_deletion(body=body, patch=patch)

        # Once all handlers have succeeded at least once for any reason, or if there were none,
        # prevent further resume-handlers (which otherwise happens on each watch-stream re-listing).
        memory.fully_handled_once = True

    # Informational causes just print the log lines.
    if cause.reason == handlers_.Reason.GONE:
        logger.debug("Deleted, really deleted, and we are notified.")

    if cause.reason == handlers_.Reason.FREE:
        logger.debug("Deletion event, but we are done with it, and we do not care.")

    if cause.reason == handlers_.Reason.NOOP:
        logger.debug("Something has changed, but we are not interested (the essence is the same).")

    # The delay is then consumed by the main handling routine (in different ways).
    return delays
