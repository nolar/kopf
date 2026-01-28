import asyncio
import contextlib
import sys
from collections.abc import Iterator


@contextlib.contextmanager
def proper_loop(suggested_loop: asyncio.AbstractEventLoop | None = None) -> Iterator[asyncio.AbstractEventLoop | None]:
    """
    Ensure that we have the proper loop, either suggested or properly managed.

    A "properly managed" loop is the one we own and therefore close.
    If ``uvloop`` is installed, it is used.
    Otherwise, the event loop policy remains unaffected.

    This loop manager is usually used in CLI only, not deeper than that;
    i.e. not even in ``kopf.run()``, since uvloop is only auto-managed for CLI.
    """
    # Event loop policies were deprecated in 3.14 entirely. Yet they still exist in older versions.
    # However, the asyncio.Runner was introduced in Python 3.11, so we can use the logic from there.
    if suggested_loop is not None:
        yield suggested_loop

    elif sys.version_info >= (3, 11):  # optional in 3.11-3.13, mandatory in >=3.14
        # Use uvloop if available by injecting it as the selected loop.
        try:
            import uvloop
        except ImportError:
            pass
        else:
            with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
                yield runner.get_loop()
                return

        # Use the default loop/runner in place, do not inject anything.
        yield None

    # For Python<=3.10, use the event-loop-policy-based injection.
    else:
        original_policy = asyncio.get_event_loop_policy()
        if suggested_loop is None:  # the pure CLI use, not a KopfRunner or other code
            try:
                import uvloop
            except ImportError:
                pass
            else:
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        try:
            yield None

        finally:
            try:
                import uvloop
            except ImportError:
                pass
            else:
                asyncio.set_event_loop_policy(original_policy)
