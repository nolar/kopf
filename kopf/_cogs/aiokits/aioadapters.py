import asyncio
import concurrent.futures
import threading
from typing import Any

from kopf._cogs.aiokits import aiotasks

Flag = aiotasks.Future | asyncio.Event | concurrent.futures.Future[Any] | threading.Event


async def wait_flag(
        flag: Flag | None,
) -> Any:
    """
    Wait for a flag to be raised.

    Non-asyncio primitives are generally not our worry,
    but we support them for convenience.
    """
    match flag:
        case None:
            return None
        case asyncio.Future():
            return await flag
        case asyncio.Event():
            return await flag.wait()
        case concurrent.futures.Future():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, flag.result)
        case threading.Event():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, flag.wait)
        case _:
            raise TypeError(f"Unsupported type of a flag: {flag!r}")


async def raise_flag(
        flag: Flag | None,
) -> None:
    """
    Raise a flag.

    Non-asyncio primitives are generally not our worry,
    but we support them for convenience.
    """
    match flag:
        case None:
            return None
        case asyncio.Future():
            flag.set_result(None)
        case asyncio.Event():
            flag.set()
        case concurrent.futures.Future():
            flag.set_result(None)
        case threading.Event():
            flag.set()
        case _:
            raise TypeError(f"Unsupported type of a flag: {flag!r}")


def check_flag(
        flag: Flag | None,
) -> bool | None:
    """
    Check if a flag is raised.
    """
    match flag:
        case None:
            return None
        case asyncio.Future():
            return flag.done()
        case asyncio.Event():
            return flag.is_set()
        case concurrent.futures.Future():
            return flag.done()
        case threading.Event():
            return flag.is_set()
        case _:
            raise TypeError(f"Unsupported type of a flag: {flag!r}")
