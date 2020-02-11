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
from typing import Optional

from kopf.clients import patching
from kopf.engines import logging as logging_engine
from kopf.engines import posting
from kopf.engines import sleeping
from kopf.reactor import causation
from kopf.reactor import errors
from kopf.reactor import handling
from kopf.reactor import lifecycles
from kopf.reactor import registries
from kopf.reactor import states
from kopf.structs import bodies
from kopf.structs import containers
from kopf.structs import finalizers
from kopf.structs import lastseen
from kopf.structs import patches
from kopf.structs import resources


async def process_resource_event(
        lifecycle: lifecycles.LifeCycleFn,
        registry: registries.OperatorRegistry,
        memories: containers.ResourceMemories,
        resource: resources.Resource,
        event: bodies.Event,
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
    body: bodies.Body = event['object']
    patch: patches.Patch = patches.Patch()
    delay: Optional[float] = None

    # Each object has its own prefixed logger, to distinguish parallel handling.
    logger = logging_engine.ObjectLogger(body=body)
    posting.event_queue_loop_var.set(asyncio.get_running_loop())
    posting.event_queue_var.set(event_queue)  # till the end of this object's task.

    # Recall what is stored about that object. Share it in little portions with the consumers.
    # And immediately forget it if the object is deleted from the cluster (but keep in memory).
    memory = await memories.recall(body, noticed_by_listing=event['type'] is None)
    if event['type'] == 'DELETED':
        await memories.forget(body)

    # Invoke all silent spies. No causation, no progress storage is performed.
    if registry.resource_watching_handlers[resource]:
        resource_watching_cause = causation.detect_resource_watching_cause(
            event=event,
            resource=resource,
            logger=logger,
            patch=patch,
            memo=memory.user_data,
        )
        await process_resource_watching_cause(
            lifecycle=lifecycles.all_at_once,
            registry=registry,
            memory=memory,
            cause=resource_watching_cause,
        )

    # Object patch accumulator. Populated by the methods. Applied in the end of the handler.
    # Detect the cause and handle it (or at least log this happened).
    if registry.resource_changing_handlers[resource]:
        extra_fields = registry.resource_changing_handlers[resource].get_extra_fields()
        old, new, diff = lastseen.get_essential_diffs(body=body, extra_fields=extra_fields)
        resource_changing_cause = causation.detect_resource_changing_cause(
            event=event,
            resource=resource,
            logger=logger,
            patch=patch,
            old=old,
            new=new,
            diff=diff,
            memo=memory.user_data,
            initial=memory.noticed_by_listing and not memory.fully_handled_once,
        )
        delay = await process_resource_changing_cause(
            lifecycle=lifecycle,
            registry=registry,
            memory=memory,
            cause=resource_changing_cause,
        )

    # Whatever was done, apply the accumulated changes to the object.
    # But only once, to reduce the number of API calls and the generated irrelevant events.
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
        if delay > 0:
            logger.debug(f"Sleeping for {delay} seconds for the delayed handlers.")
            limited_delay = min(delay, handling.WAITING_KEEPALIVE_INTERVAL)
            unslept_delay = await sleeping.sleep_or_wait(limited_delay, replenished)
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
        memory: containers.ResourceMemory,
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
        handlers=handlers,
        cause=cause,
        state=states.State.from_scratch(handlers=handlers),
        default_errors=errors.ErrorsMode.IGNORED,
    )

    # Store the results, but not the handlers' progress.
    states.deliver_results(outcomes=outcomes, patch=cause.patch)


async def process_resource_changing_cause(
        lifecycle: lifecycles.LifeCycleFn,
        registry: registries.OperatorRegistry,
        memory: containers.ResourceMemory,
        cause: causation.ResourceChangingCause,
) -> Optional[float]:
    """
    Handle a detected cause, as part of the bigger handler routine.
    """
    logger = cause.logger
    patch = cause.patch  # TODO get rid of this alias
    body = cause.body  # TODO get rid of this alias
    delay = None
    done = None
    skip = None

    resource_changing_handlers = registry.resource_changing_handlers[cause.resource]
    requires_finalizer = resource_changing_handlers.requires_finalizer(cause=cause)
    has_finalizer = finalizers.has_finalizers(body=cause.body)

    if requires_finalizer and not has_finalizer:
        logger.debug("Adding the finalizer, thus preventing the actual deletion.")
        finalizers.append_finalizers(body=body, patch=patch)
        return None

    if not requires_finalizer and has_finalizer:
        logger.debug("Removing the finalizer, as there are no handlers requiring it.")
        finalizers.remove_finalizers(body=body, patch=patch)
        return None

    # Regular causes invoke the handlers.
    if cause.reason in causation.HANDLER_REASONS:
        title = causation.TITLES.get(cause.reason, repr(cause.reason))
        logger.debug(f"{title.capitalize()} event: %r", body)
        if cause.diff and cause.old is not None and cause.new is not None:
            logger.debug(f"{title.capitalize()} diff: %r", cause.diff)

        handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause=cause)
        state = states.State.from_body(body=cause.body, handlers=handlers)
        if handlers:
            outcomes = await handling.execute_handlers_once(
                lifecycle=lifecycle,
                handlers=handlers,
                cause=cause,
                state=state,
            )
            state = state.with_outcomes(outcomes)
            state.store(patch=cause.patch)
            states.deliver_results(outcomes=outcomes, patch=cause.patch)

            if state.done:
                logger.info(f"All handlers succeeded for {title}.")
                state.purge(patch=cause.patch, body=cause.body)

            done = state.done
            delay = state.delay
        else:
            skip = True

    # Regular causes also do some implicit post-handling when all handlers are done.
    if done or skip:
        extra_fields = registry.resource_changing_handlers[cause.resource].get_extra_fields()
        lastseen.refresh_essence(body=body, patch=patch, extra_fields=extra_fields)
        if cause.reason == causation.Reason.DELETE:
            logger.debug("Removing the finalizer, thus allowing the actual deletion.")
            finalizers.remove_finalizers(body=body, patch=patch)

        # Once all handlers have succeeded at least once for any reason, or if there were none,
        # prevent further resume-handlers (which otherwise happens on each watch-stream re-listing).
        memory.fully_handled_once = True

    # Informational causes just print the log lines.
    if cause.reason == causation.Reason.GONE:
        logger.debug("Deleted, really deleted, and we are notified.")

    if cause.reason == causation.Reason.FREE:
        logger.debug("Deletion event, but we are done with it, and we do not care.")

    if cause.reason == causation.Reason.NOOP:
        logger.debug("Something has changed, but we are not interested (the essence is the same).")

    # The delay is then consumed by the main handling routine (in different ways).
    return delay
