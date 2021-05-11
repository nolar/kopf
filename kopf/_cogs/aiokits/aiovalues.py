import asyncio
from typing import AsyncIterator, Collection, Generic, TypeVar

_T = TypeVar('_T')


class Container(Generic[_T]):

    def __init__(self) -> None:
        super().__init__()
        self.changed = asyncio.Condition()
        self._values: Collection[_T] = []  # 0..1 item

    def get_nowait(self) -> _T:  # used mostly in testing
        try:
            return next(iter(self._values))
        except StopIteration:
            raise LookupError("No value is stored in the container.") from None

    async def set(self, value: _T) -> None:
        async with self.changed:
            self._values = [value]
            self.changed.notify_all()

    async def wait(self) -> _T:
        async with self.changed:
            await self.changed.wait_for(lambda: self._values)
        try:
            return next(iter(self._values))
        except StopIteration:  # impossible because of the condition's predicate
            raise LookupError("No value is stored in the container.") from None

    async def reset(self) -> None:
        async with self.changed:
            self._values = []
            self.changed.notify_all()

    async def as_changed(self) -> AsyncIterator[_T]:
        async with self.changed:
            while True:
                try:
                    yield next(iter(self._values))
                except StopIteration:
                    pass
                await self.changed.wait()
