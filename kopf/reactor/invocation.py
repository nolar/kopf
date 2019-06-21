"""
Invoking the callbacks, including the args/kwargs preparation.

Both sync & async functions are supported, so as their partials.
Also, decorated wrappers and lambdas are recognized.
All of this goes via the same invocation logic and protocol.
"""
import asyncio
import contextvars
import functools
from typing import Callable

from kopf import config


async def invoke(
        fn: Callable,
        *args,
        **kwargs):
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
    if 'event' in kwargs:
        event = kwargs.get('event')
        kwargs.update(
            type=event['type'],
            body=event['object'],
            spec=event['object'].setdefault('spec', {}),
            meta=event['object'].setdefault('metadata', {}),
            status=event['object'].setdefault('status', {}),
            uid=event['object'].get('metadata', {}).get('uid'),
            name=event['object'].get('metadata', {}).get('name'),
            namespace=event['object'].get('metadata', {}).get('namespace'),
        )
    if 'cause' in kwargs:
        cause = kwargs.get('cause')
        kwargs.update(
            event=cause.event,
            body=cause.body,
            diff=cause.diff,
            old=cause.old,
            new=cause.new,
            patch=cause.patch,
            logger=cause.logger,
            spec=cause.body.setdefault('spec', {}),
            meta=cause.body.setdefault('metadata', {}),
            status=cause.body.setdefault('status', {}),
            uid=cause.body.get('metadata', {}).get('uid'),
            name=cause.body.get('metadata', {}).get('name'),
            namespace=cause.body.get('metadata', {}).get('namespace'),
        )

    if is_async_fn(fn):
        result = await fn(*args, **kwargs)
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


def is_async_fn(fn):
    if fn is None:
        return None
    elif isinstance(fn, functools.partial):
        return is_async_fn(fn.func)
    elif hasattr(fn, '__wrapped__'):  # @functools.wraps()
        return is_async_fn(fn.__wrapped__)
    else:
        return asyncio.iscoroutinefunction(fn)
