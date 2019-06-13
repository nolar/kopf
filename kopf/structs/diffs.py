"""
All the functions to calculate the diffs of the dicts.
"""
import collections

from typing import Any, Tuple, NewType, Generator, Sequence

DiffOp = NewType('DiffOp', str)
DiffPath = Tuple[str, ...]
DiffItem = Tuple[DiffOp, DiffPath, Any, Any]
Diff = Sequence[DiffItem]


def reduce_iter(d: Diff, path: DiffPath) -> Generator[DiffItem, None, None]:
    for op, field, old, new in d:
        if not path or tuple(field[:len(path)]) == tuple(path):
            yield (op, tuple(field[len(path):]), old, new)


def reduce(d: Diff, path: DiffPath) -> Diff:
    return tuple(reduce_iter(d, path))


def diff_iter(a: Any, b: Any, path: DiffPath = ()) -> Generator[DiffItem, None, None]:
    """
    Calculate the diff between two dicts.

    Yields the tuple of form ``(op, path, old, new)``,
    where ``op`` is either ``"add"``/``"change"``/``"remove"``,
    ``path`` is a tuple with the field names (empty tuple means root),
    and the ``old`` & ``new`` values (`None` for addition/removal).

    List values are treated as a whole, and not recursed into.
    Therefore, an addition/removal of a list item is considered
    as a change of the whole value.

    If the deep diff for lists/sets is needed, see the libraries:

    * https://dictdiffer.readthedocs.io/en/latest/
    * https://github.com/seperman/deepdiff
    * https://python-json-patch.readthedocs.io/en/latest/tutorial.html
    """
    if type(a) != type(b):
        yield ('change', path, a, b)
    elif a == b:
        pass  # to exclude the case as soon as possible
    elif isinstance(a, collections.Mapping):
        a_keys = frozenset(a.keys())
        b_keys = frozenset(b.keys())
        for key in b_keys - a_keys:
            yield ('add', path+(key,), None, b[key])
        for key in a_keys - b_keys:
            yield ('remove', path+(key,), a[key], None)
        for key in a_keys & b_keys:
            yield from diff_iter(a[key], b[key], path=path+(key,))
    else:
        yield ('change', path, a, b)


def diff(a: Any, b: Any, path: DiffPath = ()) -> Diff:
    """
    Same as `diff`, but returns the whole tuple instead of iterator.
    """
    return tuple(diff_iter(a, b, path=path))
