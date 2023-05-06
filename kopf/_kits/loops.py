import asyncio
import contextlib
from typing import Generator, Optional


@contextlib.contextmanager
def proper_loop(suggested_loop: Optional[asyncio.AbstractEventLoop] = None) -> Generator[None, None, None]:
    """
    Ensure that we have the proper loop, either suggested or properly managed.

    A "properly managed" loop is the one we own and therefore close.
    If ``uvloop`` is installed, it is used.
    Otherwise, the event loop policy remains unaffected.

    This loop manager is usually used in CLI only, not deeper than that;
    i.e. not even in ``kopf.run()``, since uvloop is only auto-managed for CLI.
    """
    original_policy = asyncio.get_event_loop_policy()
    if suggested_loop is None:  # the pure CLI use, not a KopfRunner or other code
        try:
            import uvloop
        except ImportError:
            pass
        else:
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    try:
        yield

    finally:
        try:
            import uvloop
        except ImportError:
            pass
        else:
            asyncio.set_event_loop_policy(original_policy)
