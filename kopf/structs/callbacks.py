"""
Callback signatures for typing.

Since these signatures contain a lot of copy-pasted kwargs and are
not so important for the codebase, they are moved to this separate module.
"""
import logging
from typing import Any, Callable, Collection, Coroutine, NewType, Optional, TypeVar, Union

from typing_extensions import Protocol

from kopf.structs import bodies, diffs, patches, primitives, references

# A specialised type to highlight the purpose or origin of the data of type Any,
# to not be mixed with other arbitrary Any values, where it is indeed "any".
Result = NewType('Result', object)

# An internal typing hack to show that it can be a sync fn with the result,
# or an async fn which returns a coroutine which returns the result.
# Used in sync-and-async protocols only, and never exposed to other modules.
_SyncOrAsyncResult = Union[Optional[Result], Coroutine[None, None, Optional[Result]]]

# A generic sync-or-async callable with no args/kwargs checks (unlike in protocols).
# Used for the BaseHandler and generic invocation methods (which do not care about protocols).
BaseFn = Callable[..., _SyncOrAsyncResult]


class ActivityFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            **kwargs: Any,
    ) -> _SyncOrAsyncResult: ...


class ResourceWatchingFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            type: str,
            event: bodies.RawEvent,
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: Optional[str],
            name: Optional[str],
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            resource: references.Resource,
            **kwargs: Any,
    ) -> _SyncOrAsyncResult: ...


class ResourceChangingFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: Optional[str],
            name: Optional[str],
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            diff: diffs.Diff,
            old: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            new: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            resource: references.Resource,
            **kwargs: Any,
    ) -> _SyncOrAsyncResult: ...


class ResourceDaemonSyncFn(Protocol):
    def __call__(  # lgtm[py/similar-function]  # << different mode
            self,
            *args: Any,
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: Optional[str],
            name: Optional[str],
            namespace: Optional[str],
            logger: Union[logging.Logger, logging.LoggerAdapter],
            stopped: primitives.SyncDaemonStopperChecker,  # << different type
            resource: references.Resource,
            **kwargs: Any,
    ) -> Optional[Result]: ...


class ResourceDaemonAsyncFn(Protocol):
    async def __call__(  # lgtm[py/similar-function]  # << different mode
            self,
            *args: Any,
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: Optional[str],
            name: Optional[str],
            namespace: Optional[str],
            logger: Union[logging.Logger, logging.LoggerAdapter],
            stopped: primitives.AsyncDaemonStopperChecker,  # << different type
            resource: references.Resource,
            **kwargs: Any,
    ) -> Optional[Result]: ...


ResourceDaemonFn = Union[ResourceDaemonSyncFn, ResourceDaemonAsyncFn]


class ResourceTimerFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: Optional[str],
            name: Optional[str],
            namespace: Optional[str],
            logger: Union[logging.Logger, logging.LoggerAdapter],
            resource: references.Resource,
            **kwargs: Any,
    ) -> _SyncOrAsyncResult: ...


ResourceSpawningFn = Union[ResourceDaemonFn, ResourceTimerFn]


class WhenFilterFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            type: str,
            event: bodies.RawEvent,
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: Optional[str],
            name: Optional[str],
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            diff: diffs.Diff,
            old: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            new: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            resource: references.Resource,
            **kwargs: Any,
    ) -> bool: ...


class MetaFilterFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            value: Any,
            *args: Any,
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: Optional[str],
            name: Optional[str],
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            resource: references.Resource,
            **kwargs: Any,
    ) -> bool: ...


_FnT = TypeVar('_FnT', WhenFilterFn, MetaFilterFn)


def not_(fn: _FnT) -> _FnT:
    def not_fn(*args: Any, **kwargs: Any) -> bool:
        return not fn(*args, **kwargs)
    return not_fn


def all_(fns: Collection[_FnT]) -> _FnT:
    def all_fn(*args: Any, **kwargs: Any) -> bool:
        return all(fn(*args, **kwargs) for fn in fns)
    return all_fn


def any_(fns: Collection[_FnT]) -> _FnT:
    def any_fn(*args: Any, **kwargs: Any) -> bool:
        return any(fn(*args, **kwargs) for fn in fns)
    return any_fn


def none_(fns: Collection[_FnT]) -> _FnT:
    def none_fn(*args: Any, **kwargs: Any) -> bool:
        return not any(fn(*args, **kwargs) for fn in fns)
    return none_fn
