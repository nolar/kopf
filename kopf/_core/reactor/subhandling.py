import collections.abc
import contextlib
from contextvars import ContextVar
from typing import AsyncIterator, Iterable, Optional, Set

from kopf._cogs.configs import configuration
from kopf._cogs.structs import ids
from kopf._core.actions import execution, invocation, lifecycles, progression
from kopf._core.intents import callbacks, causes, handlers as handlers_, registries

# The task-local context; propagated down the stack instead of multiple kwargs.
# Used in `@kopf.subhandler` and `kopf.execute()` to add/get the sub-handlers.
subregistry_var: ContextVar[registries.ChangingRegistry] = ContextVar('subregistry_var')
subexecuted_var: ContextVar[bool] = ContextVar('subexecuted_var')


@contextlib.asynccontextmanager
async def subhandling_context() -> AsyncIterator[None]:
    with invocation.context([
        (subregistry_var, registries.ChangingRegistry()),
        (subexecuted_var, False),
    ]):
        # Go for normal handler invocation.
        yield

        # If the sub-handlers are not called explicitly, run them implicitly
        # as if it was done inside of the handler (still under the try-finally clause).
        if not subexecuted_var.get():
            await execute()


async def execute(
        *,
        fns: Optional[Iterable[callbacks.ChangingFn]] = None,
        handlers: Optional[Iterable[handlers_.ChangingHandler]] = None,
        registry: Optional[registries.ChangingRegistry] = None,
        lifecycle: Optional[execution.LifeCycleFn] = None,
        cause: Optional[execution.Cause] = None,
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
    lifecycle = lifecycle if lifecycle is not None else execution.sublifecycle_var.get()
    lifecycle = lifecycle if lifecycle is not None else lifecycles.get_default_lifecycle()
    cause = cause if cause is not None else execution.cause_var.get()
    parent_handler: execution.Handler = execution.handler_var.get()
    parent_prefix = parent_handler.id if parent_handler is not None else None

    # Validate the inputs; the function signatures cannot put these kind of restrictions, so we do.
    if len([v for v in [fns, handlers, registry] if v is not None]) > 1:
        raise TypeError("Only one of the fns, handlers, registry can be passed. Got more.")

    elif fns is not None and isinstance(fns, collections.abc.Mapping):
        subregistry = registries.ChangingRegistry()
        for id, fn in fns.items():
            real_id = registries.generate_id(fn=fn, id=id, prefix=parent_prefix)
            handler = handlers_.ChangingHandler(
                fn=fn, id=real_id, param=None,
                errors=None, timeout=None, retries=None, backoff=None,
                selector=None, labels=None, annotations=None, when=None,
                initial=None, deleted=None, requires_finalizer=None,
                reason=None, field=None, value=None, old=None, new=None,
                field_needs_change=None,
            )
            subregistry.append(handler)

    elif fns is not None and isinstance(fns, collections.abc.Iterable):
        subregistry = registries.ChangingRegistry()
        for fn in fns:
            real_id = registries.generate_id(fn=fn, id=None, prefix=parent_prefix)
            handler = handlers_.ChangingHandler(
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
        subregistry = registries.ChangingRegistry()
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
    if not isinstance(cause, causes.ChangingCause):
        raise RuntimeError("Sub-handlers of event-handlers are not supported and have "
                           "no practical use (there are no retries or state tracking).")

    # Execute the real handlers (all or a few or one of them, as per the lifecycle).
    settings: configuration.OperatorSettings = execution.subsettings_var.get()
    owned_handlers = subregistry.get_resource_handlers(resource=cause.resource)
    cause_handlers = subregistry.get_handlers(cause=cause)
    storage = settings.persistence.progress_storage
    state = progression.State.from_storage(body=cause.body, storage=storage, handlers=owned_handlers)
    state = state.with_purpose(cause.reason).with_handlers(cause_handlers)
    outcomes = await execution.execute_handlers_once(
        lifecycle=lifecycle,
        settings=settings,
        handlers=cause_handlers,
        cause=cause,
        state=state,
        extra_context=subhandling_context,
    )
    state = state.with_outcomes(outcomes)
    state.store(body=cause.body, patch=cause.patch, storage=storage)
    progression.deliver_results(outcomes=outcomes, patch=cause.patch)

    # Enrich all parents with references to sub-handlers of any level deep (sub-sub-handlers, etc).
    # There is at least one container, as this function can be called only from a handler.
    subrefs_containers: Iterable[Set[ids.HandlerId]] = execution.subrefs_var.get()
    for key in state:
        for subrefs_container in subrefs_containers:
            subrefs_container.add(key)

    # Escalate `HandlerChildrenRetry` if the execute should be continued on the next iteration.
    if not state.done:
        raise execution.HandlerChildrenRetry(delay=state.delay)
