"""
Execution of pre-selected handlers, in batches or individually.

These functions are invoked from the queueing module `kopf.reactor.processing`,
where the raw watch-events are interpreted and wrapped into extended *causes*.

The handler execution can also be used in other places, such as in-memory
activities, when there is no underlying Kubernetes object to patch'n'watch.
"""
import asyncio
import collections.abc
import logging
from contextvars import ContextVar
from typing import Any, Collection, Iterable, Mapping, MutableMapping, Optional, Set, Union

from kopf.engines import loggers
from kopf.reactor import causation, invocation, lifecycles, registries
from kopf.storage import states
from kopf.structs import callbacks, configuration, dicts, diffs, handlers as handlers_, ids

DEFAULT_RETRY_DELAY = 1 * 60
""" The default delay duration for the regular exception in retry-mode. """


class PermanentError(Exception):
    """ A fatal handler error, the retries are useless. """


class TemporaryError(Exception):
    """ A potentially recoverable error, should be retried. """
    def __init__(
            self,
            __msg: Optional[str] = None,
            delay: Optional[float] = DEFAULT_RETRY_DELAY,
    ) -> None:
        super().__init__(__msg)
        self.delay = delay


class HandlerTimeoutError(PermanentError):
    """ An error for the handler's timeout (if set). """


class HandlerRetriesError(PermanentError):
    """ An error for the handler's retries exceeded (if set). """


class HandlerChildrenRetry(TemporaryError):
    """ An internal pseudo-error to retry for the next sub-handlers attempt. """


# The task-local context; propagated down the stack instead of multiple kwargs.
# Used in `@kopf.subhandler` and `kopf.execute()` to add/get the sub-handlers.
sublifecycle_var: ContextVar[Optional[lifecycles.LifeCycleFn]] = ContextVar('sublifecycle_var')
subregistry_var: ContextVar[registries.ResourceChangingRegistry] = ContextVar('subregistry_var')
subsettings_var: ContextVar[configuration.OperatorSettings] = ContextVar('subsettings_var')
subexecuted_var: ContextVar[bool] = ContextVar('subexecuted_var')
subrefs_var: ContextVar[Iterable[Set[ids.HandlerId]]] = ContextVar('subrefs_var')
handler_var: ContextVar[handlers_.BaseHandler] = ContextVar('handler_var')
cause_var: ContextVar[causation.BaseCause] = ContextVar('cause_var')


async def execute(
        *,
        fns: Optional[Iterable[callbacks.ResourceChangingFn]] = None,
        handlers: Optional[Iterable[handlers_.ResourceChangingHandler]] = None,
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
    in the per-handler registry with ``@kopf.subhandler``.

    If the call to this method for the sub-handlers is not done explicitly
    in the handler, it is done implicitly after the handler is exited.
    One way or another, it is executed for the sub-handlers.
    """

    # Restore the current context as set in the handler execution cycle.
    lifecycle = lifecycle if lifecycle is not None else sublifecycle_var.get()
    lifecycle = lifecycle if lifecycle is not None else lifecycles.get_default_lifecycle()
    cause = cause if cause is not None else cause_var.get()
    parent_handler: handlers_.BaseHandler = handler_var.get()
    parent_prefix = parent_handler.id if parent_handler is not None else None

    # Validate the inputs; the function signatures cannot put these kind of restrictions, so we do.
    if len([v for v in [fns, handlers, registry] if v is not None]) > 1:
        raise TypeError("Only one of the fns, handlers, registry can be passed. Got more.")

    elif fns is not None and isinstance(fns, collections.abc.Mapping):
        subregistry = registries.ResourceChangingRegistry()
        for id, fn in fns.items():
            real_id = registries.generate_id(fn=fn, id=id, prefix=parent_prefix)
            handler = handlers_.ResourceChangingHandler(
                fn=fn, id=real_id, param=None,
                errors=None, timeout=None, retries=None, backoff=None,
                selector=None, labels=None, annotations=None, when=None,
                initial=None, deleted=None, requires_finalizer=None,
                reason=None, field=None, value=None, old=None, new=None,
                field_needs_change=None,
            )
            subregistry.append(handler)

    elif fns is not None and isinstance(fns, collections.abc.Iterable):
        subregistry = registries.ResourceChangingRegistry()
        for fn in fns:
            real_id = registries.generate_id(fn=fn, id=None, prefix=parent_prefix)
            handler = handlers_.ResourceChangingHandler(
                fn=fn, id=real_id, param=None,
                errors=None, timeout=None, retries=None, backoff=None,
                selector=None, labels=None, annotations=None, when=None,
                initial=None, deleted=None, requires_finalizer=None,
                reason=None, field=None, value=None, old=None, new=None,
                field_needs_change=None,
            )
            subregistry.append(handler)

    elif fns is not None:
        raise ValueError(f"fns must be a mapping or an iterable, got {fns.__class__}.")

    elif handlers is not None:
        subregistry = registries.ResourceChangingRegistry()
        for handler in handlers:
            subregistry.append(handler)

    # Use the registry as is; assume that the caller knows what they do.
    elif registry is not None:
        subregistry = registry

    # Prevent double implicit execution.
    elif subexecuted_var.get():
        return

    # If no explicit args were passed, use the accumulated handlers from `@kopf.subhandler`.
    else:
        subexecuted_var.set(True)
        subregistry = subregistry_var.get()

    # The sub-handlers are only for upper-level causes, not for lower-level events.
    if not isinstance(cause, causation.ResourceChangingCause):
        raise RuntimeError("Sub-handlers of event-handlers are not supported and have "
                           "no practical use (there are no retries or state tracking).")

    # Execute the real handlers (all or few or one of them, as per the lifecycle).
    settings: configuration.OperatorSettings = subsettings_var.get()
    owned_handlers = subregistry.get_resource_handlers(resource=cause.resource)
    cause_handlers = subregistry.get_handlers(cause=cause)
    storage = settings.persistence.progress_storage
    state = states.State.from_storage(body=cause.body, storage=storage, handlers=owned_handlers)
    state = state.with_purpose(cause.reason).with_handlers(cause_handlers)
    outcomes = await execute_handlers_once(
        lifecycle=lifecycle,
        settings=settings,
        handlers=cause_handlers,
        cause=cause,
        state=state,
    )
    state = state.with_outcomes(outcomes)
    state.store(body=cause.body, patch=cause.patch, storage=storage)
    states.deliver_results(outcomes=outcomes, patch=cause.patch)

    # Enrich all parents with references to sub-handlers of any level deep (sub-sub-handlers, etc).
    # There is at least one container, as this function can be called only from a handler.
    subrefs_containers: Iterable[Set[ids.HandlerId]] = subrefs_var.get()
    for key in state:
        for subrefs_container in subrefs_containers:
            subrefs_container.add(key)

    # Escalate `HandlerChildrenRetry` if the execute should be continued on the next iteration.
    if not state.done:
        raise HandlerChildrenRetry(delay=state.delay)


async def execute_handlers_once(
        lifecycle: lifecycles.LifeCycleFn,
        settings: configuration.OperatorSettings,
        handlers: Collection[handlers_.BaseHandler],
        cause: causation.BaseCause,
        state: states.State,
        default_errors: handlers_.ErrorsMode = handlers_.ErrorsMode.TEMPORARY,
) -> Mapping[ids.HandlerId, states.HandlerOutcome]:
    """
    Call the next handler(s) from the chain of the handlers.

    Keep the record on the progression of the handlers in the object's state,
    and use it on the next invocation to determined which handler(s) to call.

    This routine is used both for the global handlers (via global registry),
    and for the sub-handlers (via a simple registry of the current handler).
    """

    # Filter and select the handlers to be executed right now, on this event reaction cycle.
    handlers_todo = [h for h in handlers if state[h.id].awakened]
    handlers_plan = lifecycle(handlers_todo, **invocation.build_kwargs(cause=cause, state=state))

    # Execute all planned (selected) handlers in one event reaction cycle, even if there are few.
    outcomes: MutableMapping[ids.HandlerId, states.HandlerOutcome] = {}
    for handler in handlers_plan:
        outcome = await execute_handler_once(
            settings=settings,
            handler=handler,
            state=state[handler.id],
            cause=cause,
            lifecycle=lifecycle,  # just a default for the sub-handlers, not used directly.
            default_errors=default_errors,
        )
        outcomes[handler.id] = outcome

    return outcomes


async def execute_handler_once(
        settings: configuration.OperatorSettings,
        handler: handlers_.BaseHandler,
        cause: causation.BaseCause,
        state: states.HandlerState,
        lifecycle: Optional[lifecycles.LifeCycleFn] = None,
        default_errors: handlers_.ErrorsMode = handlers_.ErrorsMode.TEMPORARY,
) -> states.HandlerOutcome:
    """
    Execute one and only one handler for one and only one time.

    *Execution* means not just *calling* the handler in properly set context
    (see `_call_handler`), but also interpreting its result and errors, and
    wrapping them into am `HandlerOutcome` object -- to be stored in the state.

    The *execution* can be long -- depending on how the handler is implemented.
    For daemons, it is normal to run for hours and days if needed.
    This is different from the regular handlers, which are supposed
    to be finished as soon as possible.

    This method is not supposed to raise any exceptions from the handlers:
    exceptions mean the failure of execution itself.
    """
    errors_mode = handler.errors if handler.errors is not None else default_errors
    backoff = handler.backoff if handler.backoff is not None else DEFAULT_RETRY_DELAY

    # Prevent successes/failures from posting k8s-events for resource-watching causes.
    logger: Union[logging.Logger, logging.LoggerAdapter]
    if isinstance(cause, causation.ResourceWatchingCause):
        logger = loggers.LocalObjectLogger(body=cause.body, settings=settings)
    else:
        logger = cause.logger

    # Mutable accumulator for all the sub-handlers of any level deep; populated in `kopf.execute`.
    subrefs: Set[ids.HandlerId] = set()

    # The exceptions are handled locally and are not re-raised, to keep the operator running.
    try:
        logger.debug(f"{handler} is invoked.")

        if handler.timeout is not None and state.runtime.total_seconds() >= handler.timeout:
            raise HandlerTimeoutError(f"{handler} has timed out after {state.runtime}.")

        if handler.retries is not None and state.retries >= handler.retries:
            raise HandlerRetriesError(f"{handler} has exceeded {state.retries} retries.")

        result = await invoke_handler(
            handler,
            cause=cause,
            retry=state.retries,
            started=state.started,
            runtime=state.runtime,
            settings=settings,
            lifecycle=lifecycle,  # just a default for the sub-handlers, not used directly.
            subrefs=subrefs,
        )

    # The cancellations are an excepted way of stopping the handler. Especially for daemons.
    except asyncio.CancelledError:
        logger.warning(f"{handler} is cancelled. Will escalate.")
        raise

    # Unfinished children cause the regular retry, but with less logging and event reporting.
    except HandlerChildrenRetry as e:
        logger.debug(f"{handler} has unfinished sub-handlers. Will retry soon.")
        return states.HandlerOutcome(final=False, exception=e, delay=e.delay, subrefs=subrefs)

    # Definitely a temporary error, regardless of the error strictness.
    except TemporaryError as e:
        logger.error(f"{handler} failed temporarily: %s", str(e) or repr(e))
        return states.HandlerOutcome(final=False, exception=e, delay=e.delay, subrefs=subrefs)

    # Same as permanent errors below, but with better logging for our internal cases.
    except HandlerTimeoutError as e:
        logger.error(f"%s", str(e) or repr(e))  # already formatted
        return states.HandlerOutcome(final=True, exception=e, subrefs=subrefs)
        # TODO: report the handling failure somehow (beside logs/events). persistent status?

    # Definitely a permanent error, regardless of the error strictness.
    except PermanentError as e:
        logger.error(f"{handler} failed permanently: %s", str(e) or repr(e))
        return states.HandlerOutcome(final=True, exception=e, subrefs=subrefs)
        # TODO: report the handling failure somehow (beside logs/events). persistent status?

    # Regular errors behave as either temporary or permanent depending on the error strictness.
    except Exception as e:
        if errors_mode == handlers_.ErrorsMode.IGNORED:
            logger.exception(f"{handler} failed with an exception. Will ignore.")
            return states.HandlerOutcome(final=True, subrefs=subrefs)
        elif errors_mode == handlers_.ErrorsMode.TEMPORARY:
            logger.exception(f"{handler} failed with an exception. Will retry.")
            return states.HandlerOutcome(final=False, exception=e, delay=backoff, subrefs=subrefs)
        elif errors_mode == handlers_.ErrorsMode.PERMANENT:
            logger.exception(f"{handler} failed with an exception. Will stop.")
            return states.HandlerOutcome(final=True, exception=e, subrefs=subrefs)
            # TODO: report the handling failure somehow (beside logs/events). persistent status?
        else:
            raise RuntimeError(f"Unknown mode for errors: {errors_mode!r}")

    # No errors means the handler should be excluded from future runs in this reaction cycle.
    else:
        logger.info(f"{handler} succeeded.")
        return states.HandlerOutcome(final=True, result=result, subrefs=subrefs)


async def invoke_handler(
        handler: handlers_.BaseHandler,
        *args: Any,
        cause: causation.BaseCause,
        settings: configuration.OperatorSettings,
        lifecycle: Optional[lifecycles.LifeCycleFn],
        subrefs: Set[ids.HandlerId],
        **kwargs: Any,
) -> Optional[callbacks.Result]:
    """
    Invoke one handler only, according to the calling conventions.

    Specifically, calculate the handler-specific fields (e.g. field diffs).

    Ensure the global context for this asyncio task is set to the handler and
    its cause -- for proper population of the sub-handlers via the decorators
    (see `@kopf.subhandler`).
    """

    # For the field-handlers, the old/new/diff values must match the field, not the whole object.
    if (True and  # for readable indenting
            isinstance(cause, causation.ResourceChangingCause) and
            isinstance(handler, handlers_.ResourceChangingHandler) and
            handler.field is not None):
        old = dicts.resolve(cause.old, handler.field, None)
        new = dicts.resolve(cause.new, handler.field, None)
        diff = diffs.reduce(cause.diff, handler.field)
        cause = causation.enrich_cause(cause=cause, old=old, new=new, diff=diff)

    # Store the context of the current handler, to be used in `@kopf.subhandler`,
    # and maybe other places, and consumed in the recursive `execute()` calls for the children.
    # This replaces the multiple kwargs passing through the whole call stack (easy to forget).
    with invocation.context([
        (sublifecycle_var, lifecycle),
        (subregistry_var, registries.ResourceChangingRegistry()),
        (subsettings_var, settings),
        (subexecuted_var, False),
        (subrefs_var, list(subrefs_var.get([])) + [subrefs]),
        (handler_var, handler),
        (cause_var, cause),
    ]):
        # And call it. If the sub-handlers are not called explicitly, run them implicitly
        # as if it was done inside of the handler (i.e. under try-finally block).
        result = await invocation.invoke(
            handler.fn,
            *args,
            settings=settings,
            cause=cause,
            param=handler.param,
            **kwargs,
        )

        if not subexecuted_var.get() and isinstance(cause, causation.ResourceChangingCause):
            await execute()

        # Since we know that we invoked the handler, we cast "any" result to a handler result.
        return callbacks.Result(result)
