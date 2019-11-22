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
import collections.abc
import datetime
import logging
from contextvars import ContextVar
from typing import Optional, Union, Iterable, Collection, Mapping, MutableMapping, Any

from kopf.clients import patching
from kopf.engines import logging as logging_engine
from kopf.engines import posting
from kopf.engines import sleeping
from kopf.reactor import causation
from kopf.reactor import invocation
from kopf.reactor import lifecycles
from kopf.reactor import registries
from kopf.reactor import states
from kopf.structs import bodies
from kopf.structs import containers
from kopf.structs import dicts
from kopf.structs import diffs
from kopf.structs import finalizers
from kopf.structs import lastseen
from kopf.structs import patches
from kopf.structs import primitives
from kopf.structs import resources

WAITING_KEEPALIVE_INTERVAL = 10 * 60
""" How often to wake up from the long sleep, to show liveness in the logs. """

DEFAULT_RETRY_DELAY = 1 * 60
""" The default delay duration for the regular exception in retry-mode. """


class ActivityError(Exception):
    """ An error in the activity, as caused by mandatory handlers' failures. """

    def __init__(
            self,
            msg: str,
            *,
            outcomes: Mapping[registries.HandlerId, states.HandlerOutcome],
    ) -> None:
        super().__init__(msg)
        self.outcomes = outcomes


class PermanentError(Exception):
    """ A fatal handler error, the retries are useless. """


class TemporaryError(Exception):
    """ A potentially recoverable error, should be retried. """
    def __init__(
            self,
            __msg: Optional[str] = None,
            delay: Optional[float] = DEFAULT_RETRY_DELAY,
    ):
        super().__init__(__msg)
        self.delay = delay


class HandlerTimeoutError(PermanentError):
    """ An error for the handler's timeout (if set). """


class HandlerRetriesError(PermanentError):
    """ An error for the handler's retries exceeded (if set). """


class HandlerChildrenRetry(TemporaryError):
    """ An internal pseudo-error to retry for the next sub-handlers attempt. """


# The task-local context; propagated down the stack instead of multiple kwargs.
# Used in `@kopf.on.this` and `kopf.execute()` to add/get the sub-handlers.
sublifecycle_var: ContextVar[lifecycles.LifeCycleFn] = ContextVar('sublifecycle_var')
subregistry_var: ContextVar[registries.ResourceChangingRegistry] = ContextVar('subregistry_var')
subexecuted_var: ContextVar[bool] = ContextVar('subexecuted_var')
handler_var: ContextVar[registries.BaseHandler] = ContextVar('handler_var')
cause_var: ContextVar[causation.BaseCause] = ContextVar('cause_var')


async def activity_trigger(
        *,
        lifecycle: lifecycles.LifeCycleFn,
        registry: registries.OperatorRegistry,
        activity: causation.Activity,
) -> Mapping[registries.HandlerId, registries.HandlerResult]:
    """
    Execute a handling cycle until succeeded or permanently failed.

    This mimics the behaviour of patching-watching in Kubernetes, but in-memory.
    """
    logger = logging.getLogger(f'kopf.activities.{activity.value}')

    # For the activity handlers, we have neither bodies, nor patches, just the state.
    cause = causation.ActivityCause(logger=logger, activity=activity)
    handlers = registry.get_activity_handlers(activity=activity)
    state = states.State.from_scratch(handlers=handlers)
    latest_outcomes: MutableMapping[registries.HandlerId, states.HandlerOutcome] = {}
    while not state.done:
        outcomes = await _execute_handlers(
            lifecycle=lifecycle,
            handlers=handlers,
            cause=cause,
            state=state,
        )
        latest_outcomes.update(outcomes)
        state = state.with_outcomes(outcomes)
        delay = state.delay
        if delay:
            await sleeping.sleep_or_wait(min(delay, WAITING_KEEPALIVE_INTERVAL), asyncio.Event())

    # Activities assume that all handlers must eventually succeed.
    # We raise from the 1st exception only: just to have something real in the tracebacks.
    # For multiple handlers' errors, the logs should be investigated instead.
    exceptions = [outcome.exception
                  for outcome in latest_outcomes.values()
                  if outcome.exception is not None]
    if exceptions:
        raise ActivityError("One or more handlers failed.", outcomes=latest_outcomes) \
            from exceptions[0]

    # If nothing has failed, we return identifiable results. The outcomes/states are internal.
    # The order of results is not guaranteed (the handlers can succeed on one of the retries).
    results = {handler_id: outcome.result
               for handler_id, outcome in latest_outcomes.items()
               if outcome.result is not None}
    return results


async def resource_handler(
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
    if registry.has_resource_watching_handlers(resource=resource):
        resource_watching_cause = causation.detect_resource_watching_cause(
            event=event,
            resource=resource,
            logger=logger,
            patch=patch,
            memo=memory.user_data,
        )
        await handle_resource_watching_cause(
            lifecycle=lifecycles.all_at_once,
            registry=registry,
            memory=memory,
            cause=resource_watching_cause,
        )

    # Object patch accumulator. Populated by the methods. Applied in the end of the handler.
    # Detect the cause and handle it (or at least log this happened).
    if registry.has_resource_changing_handlers(resource=resource):
        extra_fields = registry.get_extra_fields(resource=resource)
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
            requires_finalizer=registry.requires_finalizer(resource=resource, body=body),
        )
        delay = await handle_resource_changing_cause(
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
    if delay and patch:
        logger.debug(f"Sleeping was skipped because of the patch, {delay} seconds left.")
    elif delay:
        logger.debug(f"Sleeping for {delay} seconds for the delayed handlers.")
        unslept = await sleeping.sleep_or_wait(min(delay, WAITING_KEEPALIVE_INTERVAL), replenished)
        if unslept is not None:
            logger.debug(f"Sleeping was interrupted by new changes, {unslept} seconds left.")
        else:
            now = datetime.datetime.utcnow()
            dummy = patches.Patch({'status': {'kopf': {'dummy': now.isoformat()}}})
            logger.debug("Provoking reaction with: %r", dummy)
            await patching.patch_obj(resource=resource, patch=dummy, body=body)


async def handle_resource_watching_cause(
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
    handlers = registry.get_resource_watching_handlers(cause=cause)
    outcomes = await _execute_handlers(
        lifecycle=lifecycle,
        handlers=handlers,
        cause=cause,
        state=states.State.from_scratch(handlers=handlers),
        default_errors=registries.ErrorsMode.IGNORED,
    )

    # Store the results, but not the handlers' progress.
    states.deliver_results(outcomes=outcomes, patch=cause.patch)


async def handle_resource_changing_cause(
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

    # Regular causes invoke the handlers.
    if cause.reason in causation.HANDLER_REASONS:
        title = causation.TITLES.get(cause.reason, repr(cause.reason))
        logger.debug(f"{title.capitalize()} event: %r", body)
        if cause.diff and cause.old is not None and cause.new is not None:
            logger.debug(f"{title.capitalize()} diff: %r", cause.diff)

        handlers = registry.get_resource_changing_handlers(cause=cause)
        state = states.State.from_body(body=cause.body, handlers=handlers)
        if handlers:
            outcomes = await _execute_handlers(
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
        extra_fields = registry.get_extra_fields(resource=cause.resource)
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

    # For the case of a newly created object, or one that doesn't have the correct
    # finalizers, lock it to this operator. Not all newly created objects will
    # produce an 'ACQUIRE' causation event. This only happens when there are
    # mandatory deletion handlers registered for the given object, i.e. if finalizers
    # are required.
    if cause.reason == causation.Reason.ACQUIRE:
        logger.debug("Adding the finalizer, thus preventing the actual deletion.")
        finalizers.append_finalizers(body=body, patch=patch)

    # Remove finalizers from an object, since the object currently has finalizers, but
    # shouldn't, thus releasing the locking of the object to this operator.
    if cause.reason == causation.Reason.RELEASE:
        logger.debug("Removing the finalizer, as there are no handlers requiring it.")
        finalizers.remove_finalizers(body=body, patch=patch)

    # The delay is then consumed by the main handling routine (in different ways).
    return delay


async def execute(
        *,
        fns: Optional[Iterable[invocation.Invokable]] = None,
        handlers: Optional[Iterable[registries.ResourceHandler]] = None,
        registry: Optional[registries.ResourceChangingRegistry] = None,
        lifecycle: Optional[lifecycles.LifeCycleFn] = None,
        cause: Optional[causation.BaseCause] = None,
) -> None:
    """
    Execute the handlers in an isolated lifecycle.

    This function is just a public wrapper for `execute` with multiple
    ways to specify the handlers: either as the raw functions, or as the
    pre-created handlers, or as a registry (as used in the object handling).

    If no explicit functions or handlers or registry are passed,
    the sub-handlers of the current handler are assumed, as accumulated
    in the per-handler registry with ``@kopf.on.this``.

    If the call to this method for the sub-handlers is not done explicitly
    in the handler, it is done implicitly after the handler is exited.
    One way or another, it is executed for the sub-handlers.
    """

    # Restore the current context as set in the handler execution cycle.
    lifecycle = lifecycle if lifecycle is not None else sublifecycle_var.get()
    cause = cause if cause is not None else cause_var.get()
    handler: registries.BaseHandler = handler_var.get()

    # Validate the inputs; the function signatures cannot put these kind of restrictions, so we do.
    if len([v for v in [fns, handlers, registry] if v is not None]) > 1:
        raise TypeError("Only one of the fns, handlers, registry can be passed. Got more.")

    elif fns is not None and isinstance(fns, collections.abc.Mapping):
        registry = registries.ResourceChangingRegistry(prefix=handler.id if handler else None)
        for id, fn in fns.items():
            registry.register(fn=fn, id=id)

    elif fns is not None and isinstance(fns, collections.abc.Iterable):
        registry = registries.ResourceChangingRegistry(prefix=handler.id if handler else None)
        for fn in fns:
            registry.register(fn=fn)

    elif fns is not None:
        raise ValueError(f"fns must be a mapping or an iterable, got {fns.__class__}.")

    elif handlers is not None:
        registry = registries.ResourceChangingRegistry(prefix=handler.id if handler else None)
        for handler in handlers:
            registry.append(handler=handler)

    # Use the registry as is; assume that the caller knows what they do.
    elif registry is not None:
        pass

    # Prevent double implicit execution.
    elif subexecuted_var.get():
        return

    # If no explicit args were passed, implicitly use the accumulated handlers from `@kopf.on.this`.
    else:
        subexecuted_var.set(True)
        registry = subregistry_var.get()

    # The sub-handlers are only for upper-level causes, not for lower-level events.
    if not isinstance(cause, causation.ResourceChangingCause):
        raise RuntimeError("Sub-handlers of event-handlers are not supported and have "
                           "no practical use (there are no retries or state tracking).")

    # Execute the real handlers (all or few or one of them, as per the lifecycle).
    handlers = registry.get_handlers(cause=cause)
    state = states.State.from_body(body=cause.body, handlers=handlers)
    outcomes = await _execute_handlers(
        lifecycle=lifecycle,
        handlers=handlers,
        cause=cause,
        state=state,
    )
    state = state.with_outcomes(outcomes)
    state.store(patch=cause.patch)
    states.deliver_results(outcomes=outcomes, patch=cause.patch)

    # Escalate `HandlerChildrenRetry` if the execute should be continued on the next iteration.
    if not state.done:
        raise HandlerChildrenRetry(delay=state.delay)


async def _execute_handlers(
        lifecycle: lifecycles.LifeCycleFn,
        handlers: Collection[registries.BaseHandler],
        cause: causation.BaseCause,
        state: states.State,
        default_errors: registries.ErrorsMode = registries.ErrorsMode.TEMPORARY,
) -> Mapping[registries.HandlerId, states.HandlerOutcome]:
    """
    Call the next handler(s) from the chain of the handlers.

    Keep the record on the progression of the handlers in the object's state,
    and use it on the next invocation to determined which handler(s) to call.

    This routine is used both for the global handlers (via global registry),
    and for the sub-handlers (via a simple registry of the current handler).
    """

    # Filter and select the handlers to be executed right now, on this event reaction cycle.
    handlers_todo = [h for h in handlers if state[h.id].awakened]
    handlers_plan = await invocation.invoke(lifecycle, handlers_todo, cause=cause, state=state)

    # Execute all planned (selected) handlers in one event reaction cycle, even if there are few.
    outcomes: MutableMapping[registries.HandlerId, states.HandlerOutcome] = {}
    for handler in handlers_plan:
        outcome = await _execute_handler(
            handler=handler,
            state=state[handler.id],
            cause=cause,
            lifecycle=lifecycle,  # just a default for the sub-handlers, not used directly.
            default_errors=default_errors,
        )
        outcomes[handler.id] = outcome

    return outcomes


async def _execute_handler(
        handler: registries.BaseHandler,
        cause: causation.BaseCause,
        state: states.HandlerState,
        lifecycle: lifecycles.LifeCycleFn,
        default_errors: registries.ErrorsMode = registries.ErrorsMode.TEMPORARY,
) -> states.HandlerOutcome:
    """
    Execute one and only one handler.

    *Execution* means not just *calling* the handler in properly set context
    (see `_call_handler`), but also interpreting its result and errors, and
    wrapping them into am `HandlerOutcome` object -- to be stored in the state.

    This method is not supposed to raise any exceptions from the handlers:
    exceptions mean the failure of execution itself.
    """
    errors = handler.errors if handler.errors is not None else default_errors
    backoff = handler.backoff if handler.backoff is not None else DEFAULT_RETRY_DELAY

    # Prevent successes/failures from posting k8s-events for resource-watching causes.
    logger: Union[logging.Logger, logging.LoggerAdapter]
    if isinstance(cause, causation.ResourceWatchingCause):
        logger = logging_engine.LocalObjectLogger(body=cause.body)
    else:
        logger = cause.logger

    # The exceptions are handled locally and are not re-raised, to keep the operator running.
    try:
        logger.debug(f"Invoking handler {handler.id!r}.")

        if handler.timeout is not None and state.runtime.total_seconds() >= handler.timeout:
            raise HandlerTimeoutError(f"Handler {handler.id!r} has timed out after {state.runtime}.")

        if handler.retries is not None and state.retries >= handler.retries:
            raise HandlerRetriesError(f"Handler {handler.id!r} has exceeded {state.retries} retries.")

        result = await _call_handler(
            handler,
            cause=cause,
            retry=state.retries,
            started=state.started,
            runtime=state.runtime,
            lifecycle=lifecycle,  # just a default for the sub-handlers, not used directly.
        )

    # Unfinished children cause the regular retry, but with less logging and event reporting.
    except HandlerChildrenRetry as e:
        logger.debug(f"Handler {handler.id!r} has unfinished sub-handlers. Will retry soon.")
        return states.HandlerOutcome(final=False, exception=e, delay=e.delay)

    # Definitely a temporary error, regardless of the error strictness.
    except TemporaryError as e:
        logger.error(f"Handler {handler.id!r} failed temporarily: %s", str(e) or repr(e))
        return states.HandlerOutcome(final=False, exception=e, delay=e.delay)

    # Same as permanent errors below, but with better logging for our internal cases.
    except HandlerTimeoutError as e:
        logger.error(f"%s", str(e) or repr(e))  # already formatted
        return states.HandlerOutcome(final=True, exception=e)
        # TODO: report the handling failure somehow (beside logs/events). persistent status?

    # Definitely a permanent error, regardless of the error strictness.
    except PermanentError as e:
        logger.error(f"Handler {handler.id!r} failed permanently: %s", str(e) or repr(e))
        return states.HandlerOutcome(final=True, exception=e)
        # TODO: report the handling failure somehow (beside logs/events). persistent status?

    # Regular errors behave as either temporary or permanent depending on the error strictness.
    except Exception as e:
        if errors == registries.ErrorsMode.IGNORED:
            logger.exception(f"Handler {handler.id!r} failed with an exception. Will ignore.")
            return states.HandlerOutcome(final=True)
        elif errors == registries.ErrorsMode.TEMPORARY:
            logger.exception(f"Handler {handler.id!r} failed with an exception. Will retry.")
            return states.HandlerOutcome(final=False, exception=e, delay=backoff)
        elif errors == registries.ErrorsMode.PERMANENT:
            logger.exception(f"Handler {handler.id!r} failed with an exception. Will stop.")
            return states.HandlerOutcome(final=True, exception=e)
            # TODO: report the handling failure somehow (beside logs/events). persistent status?
        else:
            raise RuntimeError(f"Unknown mode for errors: {errors!r}")

    # No errors means the handler should be excluded from future runs in this reaction cycle.
    else:
        logger.info(f"Handler {handler.id!r} succeeded.")
        return states.HandlerOutcome(final=True, result=result)


async def _call_handler(
        handler: registries.BaseHandler,
        *args: Any,
        cause: causation.BaseCause,
        lifecycle: lifecycles.LifeCycleFn,
        **kwargs: Any,
) -> Optional[registries.HandlerResult]:
    """
    Invoke one handler only, according to the calling conventions.

    Specifically, calculate the handler-specific fields (e.g. field diffs).

    Ensure the global context for this asyncio task is set to the handler and
    its cause -- for proper population of the sub-handlers via the decorators
    (see `@kopf.on.this`).
    """

    # For the field-handlers, the old/new/diff values must match the field, not the whole object.
    if (True and  # for readable indenting
            isinstance(cause, causation.ResourceChangingCause) and
            isinstance(handler, registries.ResourceHandler) and
            handler.field is not None):
        old = dicts.resolve(cause.old, handler.field, None, assume_empty=True)
        new = dicts.resolve(cause.new, handler.field, None, assume_empty=True)
        diff = diffs.reduce(cause.diff, handler.field)
        cause = causation.enrich_cause(cause=cause, old=old, new=new, diff=diff)

    # Store the context of the current resource-object-event-handler, to be used in `@kopf.on.this`,
    # and maybe other places, and consumed in the recursive `execute()` calls for the children.
    # This replaces the multiple kwargs passing through the whole call stack (easy to forget).
    with invocation.context([
        (sublifecycle_var, lifecycle),
        (subregistry_var, registries.ResourceChangingRegistry(prefix=handler.id)),
        (subexecuted_var, False),
        (handler_var, handler),
        (cause_var, cause),
    ]):
        # And call it. If the sub-handlers are not called explicitly, run them implicitly
        # as if it was done inside of the handler (i.e. under try-finally block).
        result = await invocation.invoke(
            handler.fn,
            *args,
            cause=cause,
            **kwargs,
        )

        if not subexecuted_var.get() and isinstance(cause, causation.ResourceChangingCause):
            await execute()

        # Since we know that we invoked the handler, we cast "any" result to a handler result.
        return registries.HandlerResult(result)
