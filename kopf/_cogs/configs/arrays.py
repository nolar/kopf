from collections.abc import Callable, Iterator, Mapping, MutableMapping
from typing import Generic, TypeVar

from kopf._cogs.structs import references

VT = TypeVar('VT')

# All shortcuts as used in the handler decorators and parsed by Selector(…):
#   …['kopf.dev/kopfexamples']
#   …['kopf.dev', 'kopfexamples']
#   …['kopf.dev', 'v1', 'kopfexamples']
#   …[kopf.EVERYTHING]
#   …['kopf.dev', kopf.EVERYTHING]
#   …['kopf.dev', 'v1', kopf.EVERYTHING]
#   …[lambda res: res.plural == 'kopfexamples']
SelectorKey = (
    str | references.Marker |
    tuple[str, str | references.Marker] |
    tuple[str, str, str | references.Marker] |
    Callable[[references.Resource], bool]
)


# Not exposed to users anyway, so they should not see or use this name in their code.
class SelectorMapping(MutableMapping[SelectorKey | references.Selector, VT], Generic[VT]):
    """
    A syntax helper to map any resource selectors to values.

    Used in configuration for ``settings.watching.{label|field}_selectors.

    Usage::

        settings.watching.label_selectors['kopf.dev/kopfexamples'] = '…'
        settings.watching.label_selectors['kopf.dev', 'kopfexamples'] = '…'
        settings.watching.field_selectors[Selector('kopf.dev', 'kex')] = '…'

    However, when iterated, yields only the parsed :class:`Selector` keys.

    As with any shortcut selector, the last name is checked against all names
    of a resource under question, including the plural and singular names,
    its kind, and its aliases/shortcuts. This is sufficient for most use-cases.
    To be specific, use kwargs: ``…[Selector('kopf.dev', shortcut='kex')]``.
    """
    _items: dict[references.Selector, VT]

    def __init__(self, items: Mapping[references.Selector, VT] | None = None) -> None:
        super().__init__()
        self._items = dict(items or {})

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._items!r})"

    def __setitem__(self, key: SelectorKey | references.Selector, value: VT) -> None:
        match key:
            case references.Selector():
                self._items[key] = value
            case str() | references.Marker():
                self._items[references.Selector(key)] = value
            case tuple():
                self._items[references.Selector(*key)] = value
            case _ if callable(key):
                self._items[references.Selector(key)] = value
            case _:
                raise TypeError(f"Unsupported selector type: {key!r}")

    def __delitem__(self, key: SelectorKey | references.Selector) -> None:
        match key:
            case references.Selector():
                del self._items[key]
            case str() | references.Marker():
                del self._items[references.Selector(key)]
            case tuple():
                del self._items[references.Selector(*key)]
            case _ if callable(key):
                del self._items[references.Selector(key)]
            case _:
                raise TypeError(f"Unsupported selector type: {key!r}")

    def __getitem__(self, key: SelectorKey | references.Selector) -> VT:
        match key:
            case references.Selector():
                return self._items[key]
            case str() | references.Marker():
                return self._items[references.Selector(key)]
            case tuple():
                return self._items[references.Selector(*key)]
            case _ if callable(key):
                return self._items[references.Selector(key)]
            case _:
                raise TypeError(f"Unsupported selector type: {key!r}")

    def __contains__(self, key: object) -> bool:
        match key:
            case references.Selector():
                return key in self._items
            case str() | references.Marker():
                return references.Selector(key) in self._items
            case tuple():
                return references.Selector(*key) in self._items
            case _ if callable(key):
                return references.Selector(key) in self._items
            case _:
                raise TypeError(f"Unsupported selector type: {key!r}")

    def __iter__(self) -> Iterator[references.Selector]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def collect(self, resource: references.Resource) -> list[VT]:
        """
        Collect all applicable values for a specific real resource.
        """
        results: list[VT] = []
        for selector, value in self._items.items():
            if selector.check(resource):
                results.append(value)
        return results
