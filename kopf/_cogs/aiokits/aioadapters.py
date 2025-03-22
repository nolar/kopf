import asyncio
import concurrent.futures
import threading
from typing import TYPE_CHECKING, Any, Optional, Union

from kopf._cogs.aiokits import aiotasks

if TYPE_CHECKING:
    concurrent_Future = concurrent.futures.Future[Any]
else:
    concurrent_Future = concurrent.futures.Future  # Python<=3.8

Flag = Union[aiotasks.Future, asyncio.Event, concurrent_Future, threading.Event]


async def wait_flag(
        flag: Optional[Flag],
) -> Any:
    """
    Wait for a flag to be raised.

    Non-asyncio primitives are generally not our worry,
    but we support them for convenience.
    """
    if flag is None:
        return None
    elif isinstance(flag, asyncio.Future):
        return await flag
    elif isinstance(flag, asyncio.Event):
        return await flag.wait()
    elif isinstance(flag, concurrent.futures.Future):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, flag.result)
    elif isinstance(flag, threading.Event):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, flag.wait)
    else:
        raise TypeError(f"Unsupported type of a flag: {flag!r}")


async def raise_flag(
        flag: Optional[Flag],
) -> None:
    """
    Raise a flag.

    Non-asyncio primitives are generally not our worry,
    but we support them for convenience.
    """
    if flag is None:
        return None
    elif isinstance(flag, asyncio.Future):
        flag.set_result(None)
    elif isinstance(flag, asyncio.Event):
        flag.set()
    elif isinstance(flag, concurrent.futures.Future):
        flag.set_result(None)
    elif isinstance(flag, threading.Event):
        flag.set()
    else:
        raise TypeError(f"Unsupported type of a flag: {flag!r}")


def check_flag(
        flag: Optional[Flag],
) -> Optional[bool]:
    """
    Check if a flag is raised.
    """
    if flag is None:
        return None
    elif isinstance(flag, asyncio.Future):
        return flag.done()
    elif isinstance(flag, asyncio.Event):
        return flag.is_set()
    elif isinstance(flag, concurrent.futures.Future):
        return flag.done()
    elif isinstance(flag, threading.Event):
        return flag.is_set()
    else:
        raise TypeError(f"Unsupported type of a flag: {flag!r}")
