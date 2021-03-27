"""
Invoking the callbacks, including the args/kwargs preparation.

Both sync & async functions are supported, so as their partials.
Also, decorated wrappers and lambdas are recognized.
All of this goes via the same invocation logic and protocol.
"""
import asyncio
import contextlib
import contextvars
import functools
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, cast

from kopf.reactor import causation
from kopf.structs import callbacks, configuration
from kopf.utilities import aiotasks


@contextlib.contextmanager
def context(
        values: Iterable[Tuple[contextvars.ContextVar[Any], Any]],
) -> Iterator[None]:
    """
    A context manager to set the context variables temporarily.
    """
    tokens: List[Tuple[contextvars.ContextVar[Any], contextvars.Token[Any]]] = []
    try:
        for var, val in values:
            token = var.set(val)
            tokens.append((var, token))
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


def build_kwargs(
        cause: Optional[causation.BaseCause] = None,
        _sync: Optional[bool] = None,
        **kwargs: Any,  # includes param, retry, started, runtime, etc.
) -> Dict[str, Any]:
    """
    Expand kwargs dict with fields from the causation.
    """
    new_kwargs = {}
    new_kwargs.update(kwargs)

    # Add aliases for the kwargs, directly linked to the body, or to the assumed defaults.
    if isinstance(cause, causation.BaseCause):
        new_kwargs.update(
            logger=cause.logger,
            memo=cause.memo,
        )
    if isinstance(cause, causation.ActivityCause):
        new_kwargs.update(
            activity=cause.activity,
        )
    if isinstance(cause, causation.ActivityCause) and cause.activity == cause.activity.STARTUP:
        new_kwargs.update(
            settings=cause.settings,
        )
    if isinstance(cause, causation.ResourceCause):
        new_kwargs.update(
            resource=cause.resource,
            patch=cause.patch,
            body=cause.body,
            spec=cause.body.spec,
            meta=cause.body.metadata,
            status=cause.body.status,
            uid=cause.body.metadata.uid,
            name=cause.body.metadata.name,
            namespace=cause.body.metadata.namespace,
            labels=cause.body.metadata.labels,
            annotations=cause.body.metadata.annotations,
        )
    if isinstance(cause, causation.ResourceWebhookCause):
        new_kwargs.update(
            dryrun=cause.dryrun,
            headers=cause.headers,
            sslpeer=cause.sslpeer,
            userinfo=cause.userinfo,
            warnings=cause.warnings,
        )
    if isinstance(cause, causation.ResourceWatchingCause):
        new_kwargs.update(
            event=cause.raw,
            type=cause.type,
        )
    if isinstance(cause, causation.ResourceChangingCause):
        new_kwargs.update(
            reason=cause.reason,
            diff=cause.diff,
            old=cause.old,
            new=cause.new,
        )
    if isinstance(cause, causation.DaemonCause) and _sync is not None:
        new_kwargs.update(
            stopped=cause.stopper.sync_checker if _sync else cause.stopper.async_checker,
        )

    # Inject indices in the end, so that they overwrite regular kwargs.
    # Why? For backwards compatibility: if an operator's handlers use an index e.g. "children",
    # and Kopf introduces a new kwarg "children", the code could break on the new version upgrade.
    # To prevent this, overwrite it and let the developers rename it when they want the new kwarg.
    # Naming new indices after the known kwargs harms only these developers. This is fine.
    if isinstance(cause, causation.BaseCause):
        new_kwargs.update(cause.indices)

    return new_kwargs


async def invoke(
        fn: callbacks.BaseFn,
        *args: Any,
        settings: Optional[configuration.OperatorSettings] = None,
        cause: Optional[causation.BaseCause] = None,
        **kwargs: Any,  # includes param, retry, started, runtime, etc.
) -> Any:
    """
    Invoke a single function, but safely for the main asyncio process.

    Used mostly for handler functions, and potentially slow & blocking code.
    Other callbacks are called directly, and are expected to be synchronous
    (such as handler-selecting (lifecycles) and resource-filtering (``when=``)).

    A full set of the arguments is provided, expanding the cause to some easily
    usable aliases. The function is expected to accept ``**kwargs`` for the args
    that it does not use -- for forward compatibility with the new features.

    The synchronous methods are executed in the executor (threads or processes),
    thus making it non-blocking for the main event loop of the operator.
    See: https://pymotw.com/3/asyncio/executors.html
    """
    if is_async_fn(fn):
        kwargs = build_kwargs(cause=cause, _sync=False, **kwargs)
        result = await fn(*args, **kwargs)  # type: ignore
    else:
        kwargs = build_kwargs(cause=cause, _sync=True, **kwargs)

        # Not that we want to use functools, but for executors kwargs, it is officially recommended:
        # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
        real_fn = functools.partial(fn, *args, **kwargs)

        # Copy the asyncio context from current thread to the handlr's thread.
        # It can be copied 2+ times if there are sub-sub-handlers (rare case).
        context = contextvars.copy_context()
        real_fn = functools.partial(context.run, real_fn)

        # Prevent orphaned threads during daemon/handler cancellation. It is better to be stuck
        # in the task than to have orphan threads which deplete the executor's pool capacity.
        # Cancellation is postponed until the thread exits, but it happens anyway (for consistency).
        # Note: the docs say the result is a future, but typesheds say it is a coroutine => cast()!
        loop = asyncio.get_event_loop()
        executor = settings.execution.executor if settings is not None else None
        future = cast(aiotasks.Future, loop.run_in_executor(executor, real_fn))
        cancellation: Optional[asyncio.CancelledError] = None
        while not future.done():
            try:
                await asyncio.shield(future)  # slightly expensive: creates tasks
            except asyncio.CancelledError as e:
                cancellation = e
        if cancellation is not None:
            raise cancellation
        result = future.result()

    return result


def is_async_fn(
        fn: Optional[callbacks.BaseFn],
) -> bool:
    if fn is None:
        return False
    elif isinstance(fn, functools.partial):
        return is_async_fn(fn.func)
    elif hasattr(fn, '__wrapped__'):  # @functools.wraps()
        return is_async_fn(fn.__wrapped__)  # type: ignore
    else:
        return asyncio.iscoroutinefunction(fn)
