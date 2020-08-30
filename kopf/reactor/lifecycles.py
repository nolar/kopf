"""
Few simple lifecycles for the handlers.

New lifecycles can be implemented the same way: accept ``handlers``
in the order they are registered (except those already succeeded),
and return the list of handlers in the order and amount to be executed.

The default behaviour of the framework is the most simplistic:
execute in the order they are registered, one by one.
"""
import logging
import random
from typing import Any, Optional, Sequence

from typing_extensions import Protocol

from kopf.storage import states
from kopf.structs import handlers as handlers_

logger = logging.getLogger(__name__)

Handlers = Sequence[handlers_.BaseHandler]


class LifeCycleFn(Protocol):
    """
    A callback type for handlers selection based on the event/cause.

    It is basically `Invokable` extended with an additional positional parameter
    and specific return type. But we cannot express it with `typing`.
    For the names and types of kwargs, see `Invokable`.
    """
    def __call__(
            self,
            handlers: Handlers,
            *,
            state: states.State,
            **kwargs: Any,
    ) -> Handlers: ...


def all_at_once(handlers: Handlers, **kwargs: Any) -> Handlers:
    """ Execute all handlers at once, in one event reaction cycle, if possible. """
    return handlers


def one_by_one(handlers: Handlers, **kwargs: Any) -> Handlers:
    """ Execute handlers one at a time, in the order they were registered. """
    return handlers[:1]


def randomized(handlers: Handlers, **kwargs: Any) -> Handlers:
    """ Execute one handler at a time, in the random order. """
    return [random.choice(handlers)] if handlers else []


def shuffled(handlers: Handlers, **kwargs: Any) -> Handlers:
    """ Execute all handlers at once, but in the random order. """
    return random.sample(handlers, k=len(handlers)) if handlers else []


def asap(handlers: Handlers, *, state: states.State, **kwargs: Any) -> Handlers:
    """ Execute one handler at a time, skip on failure, try the next one, retry after the full cycle. """

    def keyfn(handler: handlers_.BaseHandler) -> int:
        return state[handler.id].retries or 0

    return sorted(handlers, key=keyfn)[:1]


_default_lifecycle: LifeCycleFn = asap


def get_default_lifecycle() -> LifeCycleFn:
    return _default_lifecycle


def set_default_lifecycle(lifecycle: Optional[LifeCycleFn]) -> None:
    global _default_lifecycle
    if _default_lifecycle is not None:
        logger.warning(f"The default lifecycle is already set to {_default_lifecycle}, overriding it to {lifecycle}.")
    _default_lifecycle = lifecycle if lifecycle is not None else asap
