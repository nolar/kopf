"""
Helpers for orchestrating asyncio tasks.

These utilities only support tasks, not more generic futures, coroutines,
or other awaitables. In most case where we use it, we need specifically tasks,
as we not only wait for them, but also cancel them.

Anyway, ``asyncio`` wraps all awaitables and coroutines into tasks on almost
all function calls with multiple awaiables (e.g. :func:`asyncio.wait`),
so there is no added overhead; intstead, the implicit overhead is made explicit.
"""
import asyncio
import sys
from typing import TYPE_CHECKING, Any, Awaitable, Generator, Optional, TypeVar, Union

_T = TypeVar('_T')

# A workaround for a difference in tasks at runtime and type-checking time.
# Otherwise, at runtime: TypeError: 'type' object is not subscriptable.
if TYPE_CHECKING:
    Future = asyncio.Future[Any]
    Task = asyncio.Task[Any]
else:
    Future = asyncio.Future
    Task = asyncio.Task

# Accept `name=` always, but simulate it for Python 3.7 to do nothing.
if sys.version_info >= (3, 8):
    create_task = asyncio.create_task
else:
    def create_task(
            coro: Union[Generator[Any, None, _T], Awaitable[_T]],
            *,
            name: Optional[str] = None,  # noqa
    ) -> Task:
        return asyncio.create_task(coro)
