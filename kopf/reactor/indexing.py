from typing import Any, Dict, Generic, Iterable, Iterator, Optional, Tuple, TypeVar

from kopf.structs import ephemera, handlers, references

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
