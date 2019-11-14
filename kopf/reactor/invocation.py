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
from typing import Optional, Any, Union, List, Iterable, Iterator, Tuple

from kopf import config
from kopf.reactor import causation
from kopf.reactor import lifecycles
from kopf.reactor import registries
from kopf.structs import dicts

Invokable = Union[
    lifecycles.LifeCycleFn,
    registries.ActivityHandlerFn,
    registries.ResourceHandlerFn,
]


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


async def invoke(
        fn: Invokable,
        *args: Any,
        cause: Optional[causation.BaseCause] = None,
        **kwargs: Any,
) -> Any:
    """
    Invoke a single function, but safely for the main asyncio process.

    Used both for the handler functions and for the lifecycle callbacks.

    A full set of the arguments is provided, expanding the cause to some easily
    usable aliases. The function is expected to accept ``**kwargs`` for the args
    that it does not use -- for forward compatibility with the new features.

    The synchronous methods are executed in the executor (threads or processes),
    thus making it non-blocking for the main event loop of the operator.
    See: https://pymotw.com/3/asyncio/executors.html
    """

    # Add aliases for the kwargs, directly linked to the body, or to the assumed defaults.
    if isinstance(cause, causation.BaseCause):
        kwargs.update(
            cause=cause,
            logger=cause.logger,
        )
    if isinstance(cause, causation.ActivityCause):
        kwargs.update(
            activity=cause.activity,
        )
    if isinstance(cause, causation.ResourceCause):
        kwargs.update(
            patch=cause.patch,
            memo=cause.memo,
            body=cause.body,
            spec=dicts.DictView(cause.body, 'spec'),
            meta=dicts.DictView(cause.body, 'metadata'),
            status=dicts.DictView(cause.body, 'status'),
            uid=cause.body.get('metadata', {}).get('uid'),
            name=cause.body.get('metadata', {}).get('name'),
            namespace=cause.body.get('metadata', {}).get('namespace'),
        )
    if isinstance(cause, causation.ResourceWatchingCause):
        kwargs.update(
            event=cause.raw,
            type=cause.type,
        )
    if isinstance(cause, causation.ResourceChangingCause):
        kwargs.update(
            event=cause.reason,  # deprecated; kept for backward-compatibility
            reason=cause.reason,
            diff=cause.diff,
            old=cause.old,
            new=cause.new,
        )

    if is_async_fn(fn):
        result = await fn(*args, **kwargs)  # type: ignore
    else:

        # Not that we want to use functools, but for executors kwargs, it is officially recommended:
        # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
        real_fn = functools.partial(fn, *args, **kwargs)

        # Copy the asyncio context from current thread to the handlr's thread.
        # It can be copied 2+ times if there are sub-sub-handlers (rare case).
        context = contextvars.copy_context()
        real_fn = functools.partial(context.run, real_fn)

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(config.WorkersConfig.get_syn_executor(), real_fn)
    return result


def is_async_fn(
        fn: Optional[Invokable],
) -> bool:
    if fn is None:
        return False
    elif isinstance(fn, functools.partial):
        return is_async_fn(fn.func)
    elif hasattr(fn, '__wrapped__'):  # @functools.wraps()
        return is_async_fn(fn.__wrapped__)  # type: ignore
    else:
        return asyncio.iscoroutinefunction(fn)
