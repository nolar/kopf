import collections.abc
import logging
from typing import Any, Dict, Generic, Iterable, Iterator, Mapping, Optional, Tuple, TypeVar, Union

from kopf.reactor import causation, handling, lifecycles, registries
from kopf.storage import states
from kopf.structs import bodies, configuration, ephemera, handlers, patches, references

Key = Tuple[references.Namespace, Optional[str], Optional[str]]
_K = TypeVar('_K')
_V = TypeVar('_V')


class Store(ephemera.Store[_V], Generic[_V]):
    """
    A specific implementation of `.ephemera.Store` usable by inxeders.

    The resources-to-values association is internal and is not exposed
    to handlers or operators. Currently, it is a dictionary
    with the keys of form ``(namespace, name, uid)`` of type `Key`,
    but the implementation can later change without notice.
    """
    __items: Dict[Key, _V]

    def __init__(self) -> None:
        super().__init__()
        self.__items = {}

    def __repr__(self) -> str:
        return repr(list(self.__items.values()))

    def __bool__(self) -> bool:
        return bool(self.__items)

    def __len__(self) -> int:
        return len(self.__items)

    def __iter__(self) -> Iterator[_V]:
        return iter(self.__items.values())

    def __contains__(self, obj: object) -> bool:
        return any(val == obj for val in self.__items.values())

    # Indexers' internal protocol. Must not be used by handlers & operators.
    def _discard(self, acckey: Key) -> None:
        try:
            del self.__items[acckey]
        except KeyError:
            pass

    # Indexers' internal protocol. Must not be used by handlers & operators.
    def _replace(self, acckey: Key, obj: _V) -> None:
        # Minimise the dict updates and rehashes for no need: only update if really changed.
        if acckey not in self.__items or self.__items[acckey] != obj:
            self.__items[acckey] = obj


class Index(ephemera.Index[_K, _V], Generic[_K, _V]):
    """
    A specific implementation of `.ephemera.Index` usable by inxeders.

    The indexers and all writing interfaces for indices are not exposed
    to handlers or operators or developers, they remain strictly internal.
    Only the read-only indices and stores are exposed.
    """
    __items: Dict[_K, Store[_V]]

    def __init__(self) -> None:
        super().__init__()
        self.__items = {}

    def __repr__(self) -> str:
        return repr(self.__items)

    def __bool__(self) -> bool:
        return bool(self.__items)

    def __len__(self) -> int:
        return len(self.__items)

    def __iter__(self) -> Iterator[_K]:
        return iter(self.__items)

    def __getitem__(self, item: _K) -> Store[_V]:
        return self.__items[item]

    def __contains__(self, item: object) -> bool:  # for performant lookups!
        return item in self.__items

    # Indexers' internal protocol. Must not be used by handlers & operators.
    def _discard(self, acckey: Key) -> None:
        # Recursively discard from all indices, and down to the actual stores.
        for item in self.__items.values():
            item._discard(acckey)
        self.__delete_empty_stores()

    # Indexers' internal protocol. Must not be used by handlers & operators.
    def _replace(self, acckey: Key, obj: Mapping[_K, _V]) -> None:

        # Discard from all stores that surely do not contain `obj` anymore.
        for ind_key, ind_val in self.__items.items():
            if ind_key not in obj:
                ind_val._discard(acckey)
        self.__delete_empty_stores()

        # Update all containers that are still related to `obj`.
        for obj_key, obj_val in obj.items():
            try:
                store = self.__items[obj_key]
            except KeyError:
                store = self.__items[obj_key] = Store()
            store._replace(acckey, obj_val)

    def __delete_empty_stores(self) -> None:
        empty_keys = {key for key, val in self.__items.items() if not val}
        for key in empty_keys:
            try:
                del self.__items[key]
            except KeyError:
                pass  # if deleted while we were calculating without locks


class OperatorIndexer:
    """
    Indexers are read-write managers of read-only and minimalistic indices.

    .. note::
        Indexers are internal to the framework, they are not exposed
        to the operator developers (except for embedded operators).
    """
    index: Index[Any, Any]

    def __init__(self) -> None:
        super().__init__()
        self.index = Index()

    def __repr__(self) -> str:
        return repr(self.index)

    def discard(self, key: Key) -> None:
        """ Remove all values of the object, and keep ready for re-indexing. """
        self.index._discard(key)

    def replace(self, key: Key, obj: object) -> None:
        """ Store/merge the object's indexing results. """
        obj = obj if isinstance(obj, collections.abc.Mapping) else {None: obj}
        self.index._replace(key, obj)


class OperatorIndexers(Dict[handlers.HandlerId, OperatorIndexer]):

    def __init__(self) -> None:
        super().__init__()
        self.indices = OperatorIndices(self)

    def ensure(self, __handlers: Iterable[handlers.ResourceIndexingHandler]) -> None:
        """
        Pre-create indices/indexers to match the existing handlers.

        Any other indices will cause a KeyError at runtime.
        This is done to control the consistency of in-memory structures.
        """
        for handler in __handlers:
            self[handler.id] = OperatorIndexer()

    def discard(
            self,
            body: bodies.Body,
    ) -> None:
        """ Remove all values of this object from all indexers. Forget it! """
        key = self.make_key(body)
        for id, indexer in self.items():
            indexer.discard(key)

    def replace(
            self,
            body: bodies.Body,
            outcomes: Mapping[handlers.HandlerId, states.HandlerOutcome],
    ) -> None:
        """ Interpret the indexing results and apply them to the indices. """
        key = self.make_key(body)

        # Store the values: either for new objects or those re-matching the filters.
        for id, outcome in outcomes.items():
            if outcome.exception is not None:
                self[id].discard(key)
            elif outcome.result is not None:
                self[id].replace(key, outcome.result)

        # Purge the values: for those stopped matching the filters.
        for id, indexer in self.items():
            if id not in outcomes:
                indexer.discard(key)

    def make_key(self, body: bodies.Body) -> Key:
        """
        Make a key to address an object in internal containers.

        The key is not exposed to the users via indices,
        so its structure and type can be safely changed any time.

        However, the key must be as lightweight as possible:
        no dataclasses or namedtuples, only builtins.

        The name and namespace do not add value on top of the uid's uniqueness.
        They are here for debugging and for those rare objects
        that have no uid but are still exposed via the K8s API
        (highly unlikely to be indexed though).
        """
        meta = body.get('metadata', {})
        return (meta.get('namespace'), meta.get('name'), meta.get('uid'))


class OperatorIndices(ephemera.Indices):
    """
    A read-only view of indices of the operator.

    This view is carried through the whole call stack of the operator
    in a cause object, and later unfolded into the kwargs of the handlers.

    Why? First, carrying the indexers creates a circular import chain:

    * "causation" requires "OperatorIndexers" from "indexing".
    * "indexing" requires "ResourceIndexingCause" from "causation".

    The chain is broken by having a separate interface: `~ephemera.Indices`,
    while the implementation remains here.

    Second, read-write indexers create a temptation to modify them
    in modules and components that should not do this.
    Only "indexing" (this module) should modify the indices via indexers.
    """

    def __init__(self, indexers: "OperatorIndexers") -> None:
        super().__init__()
        self.__indexers = indexers

    def __len__(self) -> int:
        return len(self.__indexers)

    def __iter__(self) -> Iterator[str]:
        return iter(self.__indexers)

    def __getitem__(self, id: str) -> Index[Any, Any]:
        return self.__indexers[handlers.HandlerId(id)].index

    def __contains__(self, id: object) -> bool:
        return id in self.__indexers


async def index_resource(
        *,
        indexers: OperatorIndexers,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        raw_event: bodies.RawEvent,
        logger: Union[logging.Logger, logging.LoggerAdapter],
        body: bodies.Body,
        memo: ephemera.AnyMemo,
) -> None:
    """
    Populate the indices from the received event. Log but ignore all errors.

    This is a lightweight and standalone process, which is executed before
    any real handlers are invoked. Multi-step calls are also not supported.
    If the handler fails, it fails and is never retried.

    Note: K8s-event posting is skipped for `kopf.on.event` handlers,
    as they should be silent. Still, the messages are logged normally.
    """
    if not registry._resource_indexing.has_handlers(resource=resource):
        pass
    elif raw_event['type'] == 'DELETED':
        # Do not index it if it is deleted. Just discard quickly (ASAP!).
        indexers.discard(body=body)
    else:
        # Otherwise, go for full indexing with handlers invocation with all kwargs.
        cause = causation.ResourceIndexingCause(
            resource=resource,
            indices=indexers.indices,
            logger=logger,
            patch=patches.Patch(),  # NB: not applied. TODO: get rid of it!
            body=body,
            memo=memo,
        )
        indexing_handlers = registry._resource_indexing.get_handlers(cause=cause)
        outcomes = await handling.execute_handlers_once(
            lifecycle=lifecycles.all_at_once,
            settings=settings,
            handlers=indexing_handlers,
            cause=cause,
            state=states.State.from_scratch().with_handlers(indexing_handlers),
            default_errors=handlers.ErrorsMode.IGNORED,
        )
        indexers.replace(body=body, outcomes=outcomes)
