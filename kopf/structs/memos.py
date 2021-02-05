from typing import Any, Dict, Union

# Used for type-checking of embedded operators, where it can have any type.
# It is usually of type `Memo` -- but the framework must not rely on that.
# `Memo`, despite inheritance from `object`, is added to enable IDE completions.
AnyMemo = Union["Memo", object]


class Memo(Dict[Any, Any]):
    """
    A container to hold arbitrary keys-values assigned by operator developers.

    It is used in the :kwarg:`memo` kwarg to all resource handlers, isolated
    per individual resource object (not the resource kind).

    The values can be accessed either as dictionary keys (the memo is a ``dict``
    under the hood) or as object attributes (except for methods of ``dict``).

    See more in :doc:`/memories`.

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
