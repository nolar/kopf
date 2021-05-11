import asyncio
from typing import Callable, Collection, Iterable, Iterator, Optional, Set


class Toggle:
    """
    An synchronisation primitive that can be awaited both until set or cleared.

    For one-directional toggles, `asyncio.Event` is sufficient.
    But these events cannot be awaited until cleared.

    The bi-directional toggles are needed in some places in the code, such as
    in the population/depletion of a `Vault`, or as in the operator's pause.

    The optional name is used only for hinting in reprs. It can be used when
    there are many toggles, and they need to be distinguished somehow.
    """

    def __init__(
            self,
            __state: bool = False,
            *,
            name: Optional[str] = None,
            condition: Optional[asyncio.Condition] = None,
    ) -> None:
        super().__init__()
        self._condition = condition if condition is not None else asyncio.Condition()
        self._state: bool = bool(__state)
        self._name = name

    def __repr__(self) -> str:
        clsname = self.__class__.__name__
        toggled = 'on' if self._state else 'off'
        if self._name is None:
            return f'<{clsname}: {toggled}>'
        else:
            return f'<{clsname}: {self._name}: {toggled}>'

    def __bool__(self) -> bool:
        raise NotImplementedError  # to protect against accidental misuse

    def is_on(self) -> bool:
        return self._state

    def is_off(self) -> bool:
        return not self._state

    async def turn_to(self, __state: bool) -> None:
        """ Turn the toggle on/off, and wake up the tasks waiting for that. """
        async with self._condition:
            self._state = bool(__state)
            self._condition.notify_all()

    async def wait_for(self, __state: bool) -> None:
        """ Wait until the toggle is turned on/off as expected (if not yet). """
        async with self._condition:
            await self._condition.wait_for(lambda: self._state == bool(__state))

    @property
    def name(self) -> Optional[str]:
        return self._name


class ToggleSet(Collection[Toggle]):
    """
    A read-only checker for multiple toggles.

    The toggle-checker does not have its own state to be turned on/off.

    The positional argument is a function, usually :func:`any` or :func:`all`,
    which takes an iterable of all individual toggles' states (on/off),
    and calculates the overall state of the toggle set.

    With :func:`any`, the set is "on" when at least one child toggle is "on"
    (and it has at least one child), and it is "off" when all children toggles
    are "off" (or if it has no children toggles at all).

    With :func:`all`, the set is "on" when all of its children toggles are "on"
    (or it has no children at all), and it is "off" when at least one child
    toggle is "off" (and there is at least one toggle).

    The multi-toggle sets are used mostly for operator pausing,
    e.g. in peering and in index pre-population. For a practical example,
    in peering, every individual peering identified by name and namespace has
    its own individual toggle to manage, but the whole set of toggles of all
    names & namespaces is used for pausing the operator as one single toggle.
    In index pre-population, the toggles are used on the operator's startup
    to temporarily delay the actual resource handling until all index-handlers
    of all involved resources and resource kinds are processed and stored.

    Note: the set can only contain toggles that were produced by the set;
    externally produced toggles cannot be added, since they do not share
    the same condition object, which is used for synchronisation/notifications.
    """

    def __init__(self, fn: Callable[[Iterable[bool]], bool]) -> None:
        super().__init__()
        self._condition = asyncio.Condition()
        self._toggles: Set[Toggle] = set()
        self._fn = fn

    def __repr__(self) -> str:
        return repr(self._toggles)

    def __len__(self) -> int:
        return len(self._toggles)

    def __iter__(self) -> Iterator[Toggle]:
        return iter(self._toggles)

    def __contains__(self, toggle: object) -> bool:
        return toggle in self._toggles

    def __bool__(self) -> bool:
        raise NotImplementedError  # to protect against accidental misuse

    def is_on(self) -> bool:
        return self._fn(toggle.is_on() for toggle in self._toggles)

    def is_off(self) -> bool:
        return not self.is_on()

    async def wait_for(self, __state: bool) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self.is_on() == bool(__state))

    async def make_toggle(
            self,
            __val: bool = False,
            *,
            name: Optional[str] = None,
    ) -> Toggle:
        toggle = Toggle(__val, name=name, condition=self._condition)
        async with self._condition:
            self._toggles.add(toggle)
            self._condition.notify_all()
        return toggle

    async def drop_toggle(self, toggle: Toggle) -> None:
        async with self._condition:
            self._toggles.discard(toggle)
            self._condition.notify_all()

    async def drop_toggles(self, toggles: Iterable[Toggle]) -> None:
        async with self._condition:
            self._toggles.difference_update(toggles)
            self._condition.notify_all()
