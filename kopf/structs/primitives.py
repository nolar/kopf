"""
Synchronisation primitives and helper functions.
"""
import asyncio
import concurrent.futures
import threading
from typing import Optional, Union, Any

Flag = Union[asyncio.Future, asyncio.Event, concurrent.futures.Future, threading.Event]


async def wait_flag(
        flag: Optional[Flag],
) -> Any:
    """
    Wait for a flag to be raised.

    Non-asyncio primitives are generally not our worry,
    but we support them for convenience.
    """
    if flag is None:
        pass
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
        pass
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


class Toggle:
    """
    An synchronisation primitive that can be awaited both until set or cleared.

    For one-directional toggles, `asyncio.Event` is sufficient.
    But these events cannot be awaited until cleared.

    The bi-directional toggles are needed in some places in the code, such as
    in the population/depletion of a `Vault`, or as an operator's freeze-mode.
    """

    def __init__(
            self,
            __val: bool = False,
            *,
            loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        super().__init__()
        self._condition = asyncio.Condition(loop=loop)
        self._state: bool = bool(__val)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._condition._loop

    def __bool__(self) -> bool:
        """
        In the boolean context, a toggle evaluates to its current on/off state.

        An equivalent of `.is_on` / `.is_off`.
        """
        return bool(self._state)

    def is_on(self) -> bool:
        """ Check if the toggle is currently on (opposite of `.is_off`). """
        return bool(self._state)

    def is_off(self) -> bool:
        """ Check if the toggle is currently off (opposite of `.is_on`). """
        return bool(not self._state)

    async def turn_on(self) -> None:
        """ Turn the toggle on, and wake up the tasks waiting for that. """
        async with self._condition:
            self._state = True
            self._condition.notify_all()

    async def turn_off(self) -> None:
        """ Turn the toggle off, and wake up the tasks waiting for that. """
        async with self._condition:
            self._state = False
            self._condition.notify_all()

    async def wait_for_on(self) -> None:
        """ Wait until the toggle is turned on (if not yet). """
        async with self._condition:
            await self._condition.wait_for(lambda: self._state)

    async def wait_for_off(self) -> None:
        """ Wait until the toggle is turned off (if not yet). """
        async with self._condition:
            await self._condition.wait_for(lambda: not self._state)
