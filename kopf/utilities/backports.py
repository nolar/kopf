import asyncio
import sys
from typing import TYPE_CHECKING, Any, Awaitable, Generator, Optional, TypeVar, Union

_T = TypeVar('_T')

if TYPE_CHECKING:
    asyncio_Task = asyncio.Task[Any]
else:
    asyncio_Task = asyncio.Task

# Accept `name=` always, but simulate it for Python 3.7 to do nothing.
if sys.version_info >= (3, 8):
    create_task = asyncio.create_task
else:
    def create_task(
            coro: Union[Generator[Any, None, _T], Awaitable[_T]],
            *,
            name: Optional[str] = None,  # noqa
    ) -> asyncio_Task:
        return asyncio.create_task(coro)
