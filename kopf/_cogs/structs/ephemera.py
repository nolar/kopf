from collections.abc import Collection, Mapping
from typing import Any, Generic, NewType, TypeVar

# For users, memos are exposed as `Any`, though usually used with `kopf.Memo`.
# However, the framework cannot rely on any methods/properties of it, so it is
# declared as a nearly empty `object` for stricter internal type-checking.
AnyMemo = NewType('AnyMemo', object)


class Memo(dict[Any, Any]):
    """
    A container to hold arbitrary keys-values assigned by operator developers.

    It is used in the :kwarg:`memo` kwarg to all resource handlers, isolated
    per individual resource object (not the resource kind).

    The values can be accessed either as dictionary keys (the memo is a ``dict``
    under the hood) or as object attributes (except for methods of ``dict``).

    See more in :doc:`/memos`.

    >>> memo = Memo()

    >>> memo.f1 = 100
    >>> memo['f1']
    ... 100

    >>> memo['f2'] = 200
    >>> memo.f2
    ... 200

    >>> set(memo.keys())
    ... {'f1', 'f2'}
    """

    def __setattr__(self, key: str, value: Any) -> None:
        self[key] = value

    def __delattr__(self, key: str) -> None:
        try:
            del self[key]
        except KeyError as e:
            raise AttributeError(str(e))

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(str(e))


_K = TypeVar('_K')
_V = TypeVar('_V')


class Store(Collection[_V], Generic[_V]):
    """
    A collection of all values under a single unique index key.

    Multiple objects can yield the same keys, so all their values
    are accumulated into collections. When an object is deleted
    or stops matching the filters, all associated values are discarded.

    The order of values is not guaranteed.

    The values are not deduplicated, so duplicates are possible if multiple
    objects return the same values from their indexing functions.

    .. note::
        This class is only an abstract interface of an indexed store.
        The actual implementation is in `.indexing.Store`.

    .. seealso:
        :doc:`/indexing`.
    """


class Index(Mapping[_K, Store[_V]], Generic[_K, _V]):
    """
    A mapping of index keys to collections of values indexed under those keys.

    A single index is identified by a handler id and is populated by values
    usually from a single indexing function (the ``@kopf.index()`` decorator).

    .. note::
        This class is only an abstract interface of an index.
        The actual implementation is in `.indexing.Index`.

    .. seealso:
        :doc:`/indexing`.
    """


# Only an abstract interface. Implementated in `~indexing.Indices`.
Indices = Mapping[str, Index[Any, Any]]
