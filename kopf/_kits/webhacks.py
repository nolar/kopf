import functools
from typing import Any, AsyncGenerator, AsyncIterator, Callable, Dict, List, Tuple, TypeVar, cast

from kopf._cogs.structs import reviews

_SelfT = TypeVar('_SelfT')
_ServerFn = TypeVar('_ServerFn', bound=Callable[..., AsyncIterator[reviews.WebhookClientConfig]])


class WebhookContextManagerMeta(type):
    """
    Auto-decorate all ``__call__`` functions to persist their generators.

    Another way is via ``__init_subclass__``, but that requires monkey-patching
    and will not work on slotted classes (when they will be made slotted).
    """
    def __new__(
            cls,
            name: str,
            bases: Tuple[type, ...],
            namespace: Dict[str, Any],
            **kwargs: Any,
    ) -> "WebhookContextManagerMeta":
        if '__call__' in namespace:
            namespace['__call__'] = WebhookContextManager._persisted(namespace['__call__'])
        return super().__new__(cls, name, bases, namespace, **kwargs)


class WebhookContextManager(metaclass=WebhookContextManagerMeta):
    """
    A hack to make webhook servers/tunnels re-entrant.

    Generally, the webhook servers/tunnels are used only once per operator run,
    so the re-entrant servers/tunnels are not needed. This hack solves
    a problem that exists only in the unit-tests (because they are fast):

    When the garbage collection is postponed (as in PyPy), the server/tunnel
    frees the system resources (e.g. sockets) much later than the test is over
    (in ``finally:`` when the ``GeneratorExit`` is thrown into the generator).

    As a result, the resources (sockets) are released while the unrelated tests
    are running, and inject the exceptions into one of those unrelated tests
    (e.g. ``ResourceWarning: unclosed transport`` or alike).

    The obvious solution — the context managers — would break the protocol,
    which is promised to be a single callable that yields the client configs.

    To keep the backwards compatibility while cleaning up the resources on time:

    - Support the _optional_ async context manager protocol, _if_ implemented.
    - Implement a minimalistic context manager for the provided servers/tunnels.
    - Keep them fully functional when the context manager is not used.

    So, the servers/tunnels are left with the ``finally:`` block for cleanup.
    But the iterator-generator is remembered and force-closed on exit from
    the context manager — the same way as the garbage collector would close it.

    See more at :doc:`/admission`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.__generators: List[AsyncGenerator[reviews.WebhookClientConfig, None]] = []

    async def __aenter__(self: _SelfT) -> _SelfT:
        return self

    async def __aexit__(self, *_: Any) -> None:
        # The order of cleanups is probably irrelevant, but prefer LIFO just in case:
        # the innermost and latest super() calls freed first, the outermost and earliest — last.
        for generator in reversed(self.__generators):
            try:
                await generator.aclose()
            except (GeneratorExit, StopAsyncIteration, StopIteration):
                pass
        self.__generators[:] = []

    @staticmethod
    def _persisted(wrapped: _ServerFn) -> _ServerFn:
        @functools.wraps(wrapped)
        async def wrapper(
                self: "WebhookContextManager",
                fn: reviews.WebhookFn,
        ) -> AsyncIterator[reviews.WebhookClientConfig]:
            iterator = wrapped(self, fn)
            if isinstance(iterator, AsyncGenerator):
                self.__generators.append(iterator)
            async for value in iterator:
                yield value
            if isinstance(iterator, AsyncGenerator):
                self.__generators.remove(iterator)
        return cast(_ServerFn, wrapper)
