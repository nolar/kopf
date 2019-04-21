"""
Conversion of the low-level events to the high-level causes, and handling them.

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
import collections
import datetime
import logging
from contextvars import ContextVar
from typing import NamedTuple, Optional, Any, MutableMapping, Text, Callable, Iterable

import kubernetes

from kopf import events
from kopf.reactor.invocation import (
    invoke,
)
from kopf.reactor.registry import (
    CREATE, UPDATE, DELETE,
    Handler,
    Resource,
    BaseRegistry,
    SimpleRegistry,
)
from kopf.structs.diffs import (
    Diff,
    resolve,
    reduce,
)
from kopf.structs.finalizers import (
    is_deleted,
    has_finalizers,
    append_finalizers,
    remove_finalizers,
)
from kopf.structs.lastseen import (
    has_state,
    get_state_diffs,
    is_state_changed,
    refresh_last_seen_state,
)
from kopf.structs.progress import (
    is_started,
    is_sleeping,
    is_awakened,
    is_finished,
    get_retry_count,
    get_start_time,
    get_awake_time,
    set_start_time,
    set_retry_time,
    store_failure,
    store_success,
    purge_progress,
)

WAITING_KEEPALIVE_INTERVAL = 10 * 60
""" How often to wake up from the long sleep, to show the liveliness. """

DEFAULT_RETRY_DELAY = 1 * 60
""" The default delay duration for the regular exception in retry-mode. """


class ObjectLogger(logging.LoggerAdapter):
    """ An utility to prefix the per-object log messages. """
    def process(self, msg, kwargs):
        return f"[{self.extra['namespace']}/{self.extra['name']}] {msg}", kwargs


class Cause(NamedTuple):
    """
    The cause is what has caused the whole reaction as a chain of handlers.

    Unlike the low-level Kubernetes watch-events, the cause is aware
    of the actual field changes, including the multi-handlers changes.
    """
    logger: ObjectLogger
    resource: Resource
    event: Text
    body: MutableMapping
    patch: MutableMapping
    diff: Optional[Diff] = None
    old: Optional[Any] = None
    new: Optional[Any] = None


class HandlerFatalError(Exception):
    """ A fatal handler error, the reties are useless. """


class HandlerRetryError(Exception):
    """ A potentially recoverable error, should be retried. """
    def __init__(self, *args, delay=DEFAULT_RETRY_DELAY, **kwargs):
        super().__init__(*args, **kwargs)
        self.delay = delay


class HandlerTimeoutError(HandlerFatalError):
    """ An error for the handler's timeout (if set). """


class HandlerChildrenRetry(HandlerRetryError):
    """ An internal pseudo-error to retry for the next sub-handlers attempt. """


# The task-local context; propagated down the stack instead of multiple kwargs.
# Used in `@kopf.on.this` and `kopf.execute()` to add/get the sub-handlers.
sublifecycle_var: ContextVar[Callable] = ContextVar('sublifecycle_var')
subregistry_var: ContextVar[SimpleRegistry] = ContextVar('subregistry_var')
subexecuted_var: ContextVar[bool] = ContextVar('subexecuted_var')
handler_var: ContextVar[Handler] = ContextVar('handler_var')
cause_var: ContextVar[Cause] = ContextVar('cause_var')


async def custom_object_handler(
        lifecycle: Callable,
        registry: BaseRegistry,
        resource: Resource,
        event: dict,
        freeze: asyncio.Event,
) -> None:
    """
    Handle a single custom object low-level watch-event.

    Convert the low-level events, as provided by the watching/queueing tasks,
    to the high-level causes, and then call the cause-handling logic.

    All the internally provoked changes are intercepted, do not create causes,
    and therefore do not call the handling logic.
    """
    etyp = event['type']  # e.g. ADDED, MODIFIED, DELETED.
    body = event['object']

    # Each object has its own prefixed logger, to distinguish parallel handling.
    logger = ObjectLogger(logging.getLogger(__name__), extra=dict(
        namespace=body.get('metadata', {}).get('namespace', 'default'),
        name=body.get('metadata', {}).get('name', body.get('metadata', {}).get('uid', None)),
    ))

    # Object patch accumulator. Populated by the methods. Applied in the end of the handler.
    patch = {}
    delay = None

    # If the global freeze is set for the processing (i.e. other operator overrides), do nothing.
    if freeze.is_set():
        logger.debug("Ignoring the events due to freeze.")

    # The object was really deleted from the cluster. But we do not care anymore.
    elif etyp == 'DELETED':
        logger.debug("Deleted, really deleted, and we are notified.")

    # The finalizer has been just removed. We are fully done.
    elif is_deleted(body) and not has_finalizers(body):
        logger.debug("Deletion event, but we are done with it, but we do not care.")

    elif is_deleted(body):
        logger.debug("Deletion event: %r", body)
        cause = Cause(resource=resource, event=DELETE, body=body, patch=patch, logger=logger)
        try:
            await execute(lifecycle=lifecycle, registry=registry, cause=cause)
        except HandlerChildrenRetry as e:
            # on the top-level, no patches -- it is pre-patched.
            delay = e.delay
        else:
            logger.info(f"All handlers succeeded for deletion.")
            events.info(cause.body, reason='Success', message=f"All handlers succeeded for deletion.")
            logger.debug("Removing the finalizer, thus allowing the actual deletion.")
            remove_finalizers(body=body, patch=patch)

    # For a fresh new object, first block it from accidental deletions without our permission.
    # The actual handler will be called on the next call.
    elif not has_finalizers(body):
        logger.debug("First appearance: %r", body)
        logger.debug("Adding the finalizer, thus preventing the actual deletion.")
        append_finalizers(body=body, patch=patch)

    # For the object seen for the first time (i.e. just-created), call the creation handlers,
    # then mark the state as if it was seen when the creation has finished.
    elif not has_state(body):
        logger.debug("Creation event: %r", body)
        cause = Cause(resource=resource, event=CREATE, body=body, patch=patch, logger=logger)
        try:
            await execute(lifecycle=lifecycle, registry=registry, cause=cause)
        except HandlerChildrenRetry as e:
            # on the top-level, no patches -- it is pre-patched.
            delay = e.delay
        else:
            logger.info(f"All handlers succeeded for creation.")
            events.info(cause.body, reason='Success', message=f"All handlers succeeded for creation.")
            purge_progress(body=body, patch=patch)
            refresh_last_seen_state(body=body, patch=patch)

    # The previous step triggers one more patch operation without actual change. Ignore it.
    # Either the last-seen state or the status field has changed.
    elif not is_state_changed(body):
        pass

    # And what is left, is the update operation on one of the useful fields of the existing object.
    else:
        old, new, diff = get_state_diffs(body)
        logger.debug("Update event: %r", diff)
        cause = Cause(resource=resource, event=UPDATE, body=body, patch=patch, logger=logger,
                      old=old, new=new, diff=diff)
        try:
            await execute(lifecycle=lifecycle, registry=registry, cause=cause)
        except HandlerChildrenRetry as e:
            # on the top-level, no patches -- it is pre-patched.
            delay = e.delay
        else:
            logger.info(f"All handlers succeeded for update.")
            events.info(cause.body, reason='Success', message=f"All handlers succeeded for update.")
            purge_progress(body=body, patch=patch)
            refresh_last_seen_state(body=body, patch=patch)

    # Provoke a dummy change to trigger the reactor after sleep.
    # TODO: reimplement via the handler delayed statuses properly.
    if delay and not patch:
        patch.setdefault('kopf', {})['dummy'] = datetime.datetime.utcnow().isoformat()

    # Whatever was done, apply the accumulated changes to the object.
    # But only once, to reduce the number of API calls and the generated irrelevant events.
    if patch:
        logger.debug("Patching with: %r", patch)
        api = kubernetes.client.CustomObjectsApi()
        api.patch_namespaced_custom_object(
            group=resource.group,
            version=resource.version,
            plural=resource.plural,
            namespace=body['metadata']['namespace'],
            name=body['metadata']['name'],
            body=patch,
        )

    # Sleep strictly after patching, never before -- to keep the status proper.
    if delay:
        logger.info(f"Sleeping for {delay} seconds for the delayed handlers.")
        await asyncio.sleep(delay)


async def execute(
        *,
        fns: Optional[Iterable[Callable]] = None,
        handlers: Optional[Iterable[Handler]] = None,
        registry: Optional[BaseRegistry] = None,
        lifecycle: Callable = None,
        cause: Cause = None,
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
    handler = handler_var.get(None)
    cause = cause if cause is not None else cause_var.get()

    # Validate the inputs; the function signatures cannot put these kind of restrictions, so we do.
    if len([v for v in [fns, handlers, registry] if v is not None]) > 1:
        raise TypeError("Only one of the fns, handlers, registry can be passed. Got more.")

    elif fns is not None and isinstance(fns, collections.Mapping):
        registry = SimpleRegistry(prefix=handler.id if handler else None)
        for id, fn in fns.items():
            registry.register(fn=fn, id=id)

    elif fns is not None and isinstance(fns, collections.Iterable):
        registry = SimpleRegistry(prefix=handler.id if handler else None)
        for fn in fns:
            registry.register(fn=fn)

    elif fns is not None:
        raise ValueError(f"fns must be a mapping or an iterable, got {fns.__class__}.")

    elif handlers is not None:
        registry = SimpleRegistry(prefix=handler.id if handler else None)
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

    # Execute the real handlers (all or few or one of them, as per the lifecycle).
    # Raises `HandlerChildrenRetry` if the execute should be continued on the next iteration.
    await _execute(
        lifecycle=lifecycle,
        registry=registry,
        cause=cause,
    )


async def _execute(
        lifecycle: Callable,
        registry: BaseRegistry,
        cause: Cause,
        retry_on_errors: bool = True,
) -> None:
    """
    Call the next handler(s) from the chain of the handlers.

    Keep the record on the progression of the handlers in the object's status,
    and use it on the next invocation to determined which handler(s) to call.

    This routine is used both for the global handlers (via global registry),
    and for the sub-handlers (via a simple registry of the current handler).

    Raises `HandlerChildrenRetry` if there are children handlers to be executed
    on the next call, and implicitly provokes such a call by making the changes
    to the status fields (on the handler progression and number of retries).

    Exits normally if all handlers for this cause are fully done.
    """
    logger = cause.logger

    # Filter and select the handlers to be executed right now, on this event reaction cycle.
    handlers = registry.get_handlers(cause=cause)
    handlers_done = [handler for handler in handlers if is_finished(body=cause.body, handler=handler)]
    handlers_wait = [handler for handler in handlers if is_sleeping(body=cause.body, handler=handler)]
    handlers_todo = [handler for handler in handlers if is_awakened(body=cause.body, handler=handler)]
    handlers_plan = [handler for handler in await invoke(lifecycle, handlers_todo, cause=cause)]
    handlers_left = [handler for handler in handlers_todo if handler.id not in {handler.id for handler in handlers_plan}]

    # Set the timestamps -- even if not executed on this event, but just got registered.
    for handler in handlers:
        if not is_started(body=cause.body, handler=handler):
            set_start_time(body=cause.body, patch=cause.patch, handler=handler)

    # Execute all planned (selected) handlers in one event reaction cycle, even if there are few.
    for handler in handlers_plan:

        # Restore the handler's progress status. It can be useful in the handlers.
        retry = get_retry_count(body=cause.body, handler=handler)
        started = get_start_time(body=cause.body, handler=handler, patch=cause.patch)
        runtime = datetime.datetime.utcnow() - started

        # The exceptions are handled locally and are not re-raised, to keep the operator running.
        try:
            logger.debug(f"Invoking handler {handler.id!r}.")

            if handler.timeout is not None and runtime.total_seconds() > handler.timeout:
                raise HandlerTimeoutError(f"Handler {handler.id!r} has timed out after {runtime}.")

            result = await _call_handler(
                handler,
                cause=cause,
                retry=retry,
                started=started,
                runtime=runtime,
                lifecycle=lifecycle,  # just a default for the sub-handlers, not used directly.
            )

        # Unfinished children cause the regular retry, but with less logging and event reporting.
        except HandlerChildrenRetry as e:
            logger.info(f"Handler {handler.id!r} has unfinished sub-handlers. Will retry soon.")
            set_retry_time(body=cause.body, patch=cause.patch, handler=handler, delay=e.delay)
            handlers_left.append(handler)

        # Definitely retriable error, no matter what is the error-reaction mode.
        except HandlerRetryError as e:
            logger.exception(f"Handler {handler.id!r} failed with an retry exception. Will retry.")
            events.exception(cause.body, message=f"Handler {handler.id!r} failed. Will retry.")
            set_retry_time(body=cause.body, patch=cause.patch, handler=handler, delay=e.delay)
            handlers_left.append(handler)

        # Definitely fatal error, no matter what is the error-reaction mode.
        except HandlerFatalError as e:
            logger.exception(f"Handler {handler.id!r} failed with an fatal exception. Will stop.")
            events.exception(cause.body, message=f"Handler {handler.id!r} failed. Will stop.")
            store_failure(body=cause.body, patch=cause.patch, handler=handler, exc=e)
            # TODO: report the handling failure somehow (beside logs/events). persistent status?

        # Regular errors behave as either retriable or fatal depending on the error-reaction mode.
        except Exception as e:
            if retry_on_errors:
                logger.exception(f"Handler {handler.id!r} failed with an exception. Will retry.")
                events.exception(cause.body, message=f"Handler {handler.id!r} failed. Will retry.")
                set_retry_time(body=cause.body, patch=cause.patch, handler=handler, delay=DEFAULT_RETRY_DELAY)
                handlers_left.append(handler)
            else:
                logger.exception(f"Handler {handler.id!r} failed with an exception. Will stop.")
                events.exception(cause.body, message=f"Handler {handler.id!r} failed. Will stop.")
                store_failure(body=cause.body, patch=cause.patch, handler=handler, exc=e)
                # TODO: report the handling failure somehow (beside logs/events). persistent status?

        # No errors means the handler should be excluded from future runs in this reaction cycle.
        else:
            logger.info(f"Handler {handler.id!r} succeeded.")
            events.info(cause.body, reason='Success', message=f"Handler {handler.id!r} succeeded.")
            store_success(body=cause.body, patch=cause.patch, handler=handler, result=result)

    # Provoke the retry of the handling cycle if there were any unfinished handlers,
    # either because they were not selected by the lifecycle, or failed and need a retry.
    if handlers_left:
        raise HandlerChildrenRetry(delay=None)

    # If there are delayed handlers, block this object's cycle; but do keep-alives every few mins.
    # Other (non-delayed) handlers will continue as normlally, due to raise few lines above.
    # Other objects will continue as normally in their own handling asyncio tasks.
    if handlers_wait:
        times = [get_awake_time(body=cause.body, handler=handler) for handler in handlers_wait]
        until = min(times)  # the soonest awake datetime.
        delay = (until - datetime.datetime.utcnow()).total_seconds()
        delay = max(0, min(WAITING_KEEPALIVE_INTERVAL, delay))
        raise HandlerChildrenRetry(delay=delay)


async def _call_handler(
        handler: Handler,
        *args,
        cause: Cause,
        lifecycle: Callable,
        **kwargs):
    """
    Invoke one handler only, according to the calling conventions.

    Specifically, calculate the handler-specific fields (e.g. field diffs).

    Ensure the global context for this asyncio task is set to the handler and
    its cause -- for proper population of the sub-handlers via the decorators
    (see `@kopf.on.this`).
    """

    # For the field-handlers, the old/new/diff values must match the field, not the whole object.
    old = cause.old if handler.field is None else resolve(cause.old, handler.field)
    new = cause.new if handler.field is None else resolve(cause.new, handler.field)
    diff = cause.diff if handler.field is None else reduce(cause.diff, handler.field)
    cause = cause._replace(old=old, new=new, diff=diff)

    # Store the context of the current resource-object-event-handler, to be used in `@kopf.on.this`,
    # and maybe other places, and consumed in the recursive `execute()` calls for the children.
    # This replaces the multiple kwargs passing through the whole call stack (easy to forget).
    sublifecycle_token = sublifecycle_var.set(lifecycle)
    subregistry_token = subregistry_var.set(SimpleRegistry(prefix=handler.id))
    subexecuted_token = subexecuted_var.set(False)
    handler_token = handler_var.set(handler)
    cause_token = cause_var.set(cause)

    # And call it. If the sub-handlers are not called explicitly, run them implicitly
    # as if it was done inside of the handler (i.e. under try-finally block).
    try:
        result = await invoke(
            handler.fn,
            *args,
            cause=cause,
            **kwargs,
        )

        if not subexecuted_var.get():
            await execute()

        return result

    finally:
        # Reset the context to the parent's context, or to nothing (if already in a root handler).
        sublifecycle_var.reset(sublifecycle_token)
        subregistry_var.reset(subregistry_token)
        subexecuted_var.reset(subexecuted_token)
        handler_var.reset(handler_token)
        cause_var.reset(cause_token)
