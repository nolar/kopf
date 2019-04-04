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

logger = logging.getLogger(__name__)


def all_at_once(handlers, **kwargs):
    """ Execute all handlers at once, in one event reaction cycle, if possible. """
    return handlers


def one_by_one(handlers, **kwargs):
    """ Execute handlers one at a time, in the order they were registered. """
    return handlers[:1]


def randomized(handlers, **kwargs):
    """ Execute one handler at a time, in the random order. """
    return random.choice(handlers)


def shuffled(handlers, **kwargs):
    """ Execute all handlers at once, but in the random order. """
    return random.sample(handlers, k=len(handlers))


def asap(handlers, *, body, **kwargs):
    """ Execute one handler at a time, skip on failure, try the next one, retry after the full cycle. """
    retries = body.get('status', {}).get('kopf', {}).get('retries', {})
    retryfn = lambda handler: retries.get(handler.id, 0)
    return sorted(handlers, key=retryfn)[:1]


_default_lifecycle = None


def get_default_lifecycle():
    return _default_lifecycle if _default_lifecycle is not None else asap


def set_default_lifecycle(lifecycle):
    global _default_lifecycle
    if _default_lifecycle is not None:
        logger.warn(f"The default lifecycle is already set to {_default_lifecycle}, overriding it to {lifecycle}.")
    _default_lifecycle = lifecycle
