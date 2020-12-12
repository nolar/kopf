"""
Authentication-related structures.

Kopf handles some rudimentary authentication directly, and exposes the ways
to implement custom authentication methods (via `on.login` handlers).

For that, a minimally sufficient data structure is introduced -- both
to bring all the credentials together in a structured and type-annotated way,
and to receive them from the operators' login-handlers with custom auth methods.

The "rudimentary" is defined as the information passed to the HTTP protocol
and TCP/SSL connection only, i.e. everything usable in a generic HTTP client,
and nothing more than that:

* TCP server host & port.
* SSL verification/ignorance flag.
* SSL certificate authority.
* SSL client certificate and its private key.
* HTTP ``Authorization: Basic username:password``.
* HTTP ``Authorization: Bearer token`` (or other schemes: Bearer, Digest, etc).
* URL's default namespace for the cases when this is implied.

.. seealso::
    :func:`authentication` and :mod:`piggybacking`.
"""
import asyncio
import collections
import dataclasses
import random
from typing import AsyncIterable, AsyncIterator, Callable, Dict, List, \
                   Mapping, NewType, Optional, Tuple, TypeVar, cast

from kopf.structs import primitives


class LoginError(Exception):
    """ Raised when the operator cannot login to the API. """


class AccessError(Exception):
    """ Raised when the operator cannot access the cluster API. """


@dataclasses.dataclass(frozen=True)
class ConnectionInfo:
    """
    A single endpoint with specific credentials and connection flags to use.
    """
    server: str  # e.g. "https://localhost:443"
    ca_path: Optional[str] = None
    ca_data: Optional[bytes] = None
    insecure: Optional[bool] = None
    username: Optional[str] = None
    password: Optional[str] = None
    scheme: Optional[str] = None  # RFC-7235/5.1: e.g. Bearer, Basic, Digest, etc.
    token: Optional[str] = None
    certificate_path: Optional[str] = None
    certificate_data: Optional[bytes] = None
    private_key_path: Optional[str] = None
    private_key_data: Optional[bytes] = None
    default_namespace: Optional[str] = None  # used for cluster objects' k8s-events.
    priority: int = 0


_T = TypeVar('_T', bound=object)

# Usually taken from the HandlerId (also a string), but semantically it is on its own.
VaultKey = NewType('VaultKey', str)


@dataclasses.dataclass
class VaultItem:
    """
    The actual item stored in the vault. It is never exposed externally.

    Used for proper garbage collection when the key is removed from the vault
    (to avoid orchestrating extra cache structures and keeping them in sync).

    The caches are populated by `Vault.extended` on-demand.
    """
    info: ConnectionInfo
    caches: Optional[Dict[str, object]] = None


class Vault(AsyncIterable[Tuple[VaultKey, ConnectionInfo]]):
    """
    A store for currently valid authentication methods.

    *Through we call it a vault to add a sense of security.*

    Normally, only one authentication method is used at a time in multiple
    methods and tasks (e.g. resource watching/patching, peering, etc.).

    Multiple methods to represent the same principal is an unusual case,
    but it is also possible as a side effect. Same for multiple distinct
    identities of a single operator.

    The credentials store is created once for an operator (a task),
    and is then used by multiple tasks running in parallel:

    * Consumed by the API client wrappers to authenticate in the API.
    * Reported by the API client wrappers if some of the credentials fail.
    * Populated by the authenticator background task when and if needed.

    .. seealso::
        :func:`reauthenticated_request`/:func:`reauthenticated_stream`
        and :func:`authentication`.
    """
    _current: Dict[VaultKey, VaultItem]
    _invalid: Dict[VaultKey, List[VaultItem]]

    def __init__(
            self,
            __src: Optional[Mapping[str, object]] = None,
    ) -> None:
        super().__init__()
        self._current = {}
        self._invalid = collections.defaultdict(list)
        self._lock = asyncio.Lock()

        if __src is not None:
            self._update_converted(__src)

        # Mark a pre-populated vault to be usable instantly,
        # or trigger the initial authentication for an empty vault.
        self._ready = primitives.Toggle(bool(self))

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self._current!r}>'

    def __bool__(self) -> bool:
        return bool(self._current)

    async def __aiter__(
            self,
    ) -> AsyncIterator[Tuple[VaultKey, ConnectionInfo]]:
        async for key, item in self._items():
            yield key, item.info

    async def extended(
            self,
            factory: Callable[[ConnectionInfo], _T],
            purpose: Optional[str] = None,
    ) -> AsyncIterator[Tuple[VaultKey, ConnectionInfo, _T]]:
        """
        Iterate the connection info items with their cached object.

        The cached objects are identified by the purpose (an arbitrary string).
        Multiple types of objects can be cached under different names.

        The factory is a one-argument function of a `ConnectionInfo`,
        that returns the object to be cached for this connection info.
        It is called only once per item and purpose.
        """
        purpose = purpose if purpose is not None else repr(factory)
        async for key, item in self._items():
            if item.caches is None:  # quick-check with no locking overhead.
                async with self._lock:
                    if item.caches is None:  # securely synchronised check.
                        item.caches = {}
            if purpose not in item.caches:  # quick-check with no locking overhead.
                async with self._lock:
                    if purpose not in item.caches:  # securely synchronised check.
                        item.caches[purpose] = factory(item.info)
            yield key, item.info, cast(_T, item.caches[purpose])

    async def _items(
            self,
    ) -> AsyncIterator[Tuple[VaultKey, VaultItem]]:
        """
        Yield the raw items as stored in the vault in random order.

        The items are yielded until either all of them are depleted,
        or until the yielded one does not fail (no `.invalidate` call made).
        Restart on every re-authentication (if new items are added).
        """

        # Yield the connection infos until either all of them are depleted,
        # or until the yielded one does not fail (no `.invalidate` call made).
        # Restart on every re-authentication (if new items are added).
        while True:

            # Whether on the 1st run, or during the active re-authentication,
            # ensure that the items are ready before yielding them.
            await self._ready.wait_for(True)

            # Select the items to yield and let it (i.e. a consumer) work.
            async with self._lock:
                yielded_key, yielded_item = self.select()
            yield yielded_key, yielded_item

            # If the yielded item has been invalidated, assume that this item has failed.
            # Otherwise (the item is in the list), it has succeeded -- we are done.
            # Note: checked by identity, in case a similar item is re-added as a different object.
            async with self._lock:
                if yielded_key in self._current and self._current[yielded_key] is yielded_item:
                    break

    def select(self) -> Tuple[VaultKey, VaultItem]:
        """
        Select the next item (not the info!) to try (and do so infinitely).

        .. warning::
            This method is not async/await-safe: if the data change on the go,
            it can lead to improper items returned.
        """
        if not self._current:
            raise LoginError("No valid credentials are available.")
        prioritised: Dict[int, List[Tuple[VaultKey, VaultItem]]]
        prioritised = collections.defaultdict(list)
        for key, item in self._current.items():
            prioritised[item.info.priority].append((key, item))
        top_priority = max(list(prioritised.keys()))
        key, item = random.choice(prioritised[top_priority])
        return key, item

    async def invalidate(
            self,
            key: VaultKey,
            *,
            exc: Optional[Exception] = None,
    ) -> None:
        """
        Exclude the specified credentials, and re-authenticate as needed.

        Multiple calls can be made for a single authenticator and credentials,
        if used for multiple requests at the same time (a common case).
        All of them will be blocked the same way, until one and only one
        re-authentication happens in a background task. They will be
        unblocked at the same instant once the new credentials are ready.

        If the re-authentication fails in the background task, this method
        re-raises the original exception (most likely a HTTP 401 error),
        and lets the client tasks to fail in their own stack.
        The background task continues to run and tries to re-authenticate
        on the next API calls until cancelled due to the operator exit.
        """

        # Exclude the failed connection items from the list of available ones.
        # But keep a short history of invalid items, so that they are not re-added.
        async with self._lock:
            if key in self._current:
                await self._flush_caches(self._current[key])
                self._invalid[key] = self._invalid[key][-2:] + [self._current[key]]
                del self._current[key]
            need_reauth = not self._current  # i.e. nothing is left at all

        # Initiate a re-authentication activity, and block until it is finished.
        if need_reauth:
            await self._ready.turn_to(False)
            await self._ready.wait_for(True)

        # If the re-auth has failed, re-raise the original exception in the current stack.
        # If the original exception is unknown, raise normally on the next iteration's yield.
        # Keep in mind, this routine is called in parallel from many tasks for the same keys.
        async with self._lock:
            if not self._current:
                if exc is not None:
                    raise exc

    async def populate(
            self,
            __src: Mapping[str, object],
    ) -> None:
        """
        Add newly retrieved credentials.

        Used by :func:`authentication` to add newly retrieved credentials
        from the authentication activity handlers. Some of the credentials
        can be duplicates of the existing ones -- only one of them is used then.
        """

        # Remember the new info items (or replace the old ones). If we already see that the item
        # is invalid (as seen in our short per-key history), we keep it as such -- this prevents
        # consistently invalid credentials from causing infinite re-authentication again and again.
        async with self._lock:
            self._update_converted(__src)

        # Notify the consuming tasks (client wrappers) that new credentials are ready to be used.
        # Those tasks can be blocked in `vault.invalidate()` if there are no credentials left.
        await self._ready.turn_to(True)

    async def wait_for_readiness(self) -> None:
        await self._ready.wait_for(True)

    async def wait_for_emptiness(self) -> None:
        await self._ready.wait_for(False)

    async def close(self) -> None:
        """
        Finalize all the cached objects when the operator is ending.
        """
        async with self._lock:
            for key in self._current:
                await self._flush_caches(self._current[key])

    async def _flush_caches(
            self,
            item: VaultItem,
    ) -> None:
        """
        Call the finalizers and garbage-collect the cached objects.

        Mainly used to garbage-collect aiohttp sessions and its derivatives
        when the connection info items are removed from the vault -- so that
        the sessions/connectors would not complain that they were not close.

        Built-in garbage-collection is not sufficient, as it is synchronous,
        and cannot call the async coroutines like `aiohttp.ClientSession.close`.

        .. note::
            Currently, we assume the ``close()`` method only (both sync/async).
            There is no need to generalise to customizable finalizer callbacks.
            This can change in the future.
        """

        # Close the closable objects.
        if item.caches:
            for obj in item.caches.values():
                if hasattr(obj, 'close'):
                    if asyncio.iscoroutinefunction(getattr(obj, 'close')):
                        await getattr(obj, 'close')()
                    else:
                        getattr(obj, 'close')()

        # Garbage-collect other resources (e.g. files, memory, etc).
        item.caches = None

    def _update_converted(
            self,
            __src: Mapping[str, object],
    ) -> None:
        for key, info in __src.items():
            key = VaultKey(str(key))
            if not isinstance(info, ConnectionInfo):
                raise ValueError("Only ConnectionInfo instances are currently accepted.")
            if info not in [data.info for data in self._invalid[key]]:
                self._current[key] = VaultItem(info=info)
