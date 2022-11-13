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
from typing import Any, Callable, Coroutine, Iterable, Iterator, \
                   List, Mapping, Optional, Tuple, TypeVar, Union

from typing_extensions import final

from kopf._cogs.configs import configuration

# An internal typing hack shows that the handler can be sync fn with the result,
# or an async fn which returns a coroutine which, in turn, returns the result.
# Used in some protocols only and is never exposed to other modules.
_R = TypeVar('_R')
SyncOrAsync = Union[_R, Coroutine[None, None, _R]]

# A generic sync-or-async callable with no args/kwargs checks (unlike in protocols).
# Used for the Handler and generic invocation methods (which do not care about protocols).
Invokable = Callable[..., SyncOrAsync[Optional[object]]]


class Kwargable:
    """
    Something that can provide kwargs to the function invocation rotuine.

    Technically, there is only one source of kwargs in the framework --
    `Cause` and descendants across the source code (e.g. ``causes.py``).
    However, we do not want to introduce a new dependency of a low-level
    function invocation module on the specialised causation logic & structures.
    For this reason, the `Cause` & `Kwargable` classes are split.
    """

    @property
    def _kwargs(self) -> Mapping[str, Any]:
        return {}

    @property
    def _sync_kwargs(self) -> Mapping[str, Any]:
        return self._kwargs

    @property
    def _async_kwargs(self) -> Mapping[str, Any]:
        return self._kwargs

    @property
    def _super_kwargs(self) -> Mapping[str, Any]:
        return {}

    @final
    @property
    def kwargs(self) -> Mapping[str, Any]:
        return dict(self._kwargs, **self._super_kwargs)

    @final
    @property
    def sync_kwargs(self) -> Mapping[str, Any]:
        return dict(self._sync_kwargs, **self._super_kwargs)

    @final
    @property
    def async_kwargs(self) -> Mapping[str, Any]:
        return dict(self._async_kwargs, **self._super_kwargs)


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
        *,
        settings: Optional[configuration.OperatorSettings] = None,
        kwargsrc: Optional[Kwargable] = None,
        kwargs: Optional[Mapping[str, Any]] = None,  # includes param, retry, started, runtime, etc.
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
    kwargs = {} if kwargs is None else kwargs
    if is_async_fn(fn):
        kwargs = kwargs if kwargsrc is None else dict(kwargs, **kwargsrc.async_kwargs)
        result = await fn(**kwargs)  # type: ignore
    else:
        kwargs = kwargs if kwargsrc is None else dict(kwargs, **kwargsrc.sync_kwargs)

        # Not that we want to use functools, but for executors kwargs, it is officially recommended:
        # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
        real_fn = functools.partial(fn, **kwargs)

        # Copy the asyncio context from current thread to the handlr's thread.
        # It can be copied 2+ times if there are sub-sub-handlers (rare case).
        context = contextvars.copy_context()
        real_fn = functools.partial(context.run, real_fn)

        # Prevent orphaned threads during daemon/handler cancellation. It is better to be stuck
        # in the task than to have orphan threads which deplete the executor's pool capacity.
        # Cancellation is postponed until the thread exits, but it happens anyway (for consistency).
        # Note: the docs say the result is a future, but typesheds say it is a coroutine => cast()!
        loop = asyncio.get_running_loop()
        executor = settings.execution.executor if settings is not None else None
        future = loop.run_in_executor(executor, real_fn)
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
        fn: Optional[Invokable],
) -> bool:
    if fn is None:
        return False
    elif isinstance(fn, functools.partial):
        return is_async_fn(fn.func)
    elif hasattr(fn, '__wrapped__'):  # @functools.wraps()
        return is_async_fn(fn.__wrapped__)
    else:
        return asyncio.iscoroutinefunction(fn)
