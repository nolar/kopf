"""
Callback signatures for typing.

Since these signatures contain a lot of copy-pasted kwargs and are
not so important for the codebase, they are moved to this separate module.
"""
import logging
from typing import NewType, Any, Collection, Union, Optional, Callable, Coroutine, TypeVar

from typing_extensions import Protocol

from kopf.structs import bodies
from kopf.structs import diffs
from kopf.structs import patches
from kopf.structs import primitives

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
            uid: str,
            name: str,
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            **kwargs: Any,
    ) -> _SyncOrAsyncResult: ...


class ResourceChangingFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            event: str,  # DEPRECATED
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: str,
            name: str,
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            diff: diffs.Diff,
            old: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            new: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
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
            uid: str,
            name: str,
            namespace: Optional[str],
            logger: Union[logging.Logger, logging.LoggerAdapter],
            stopped: primitives.SyncDaemonStopperChecker,  # << different type
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
            uid: str,
            name: str,
            namespace: Optional[str],
            logger: Union[logging.Logger, logging.LoggerAdapter],
            stopped: primitives.AsyncDaemonStopperChecker,  # << different type
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
            uid: str,
            name: str,
            namespace: Optional[str],
            logger: Union[logging.Logger, logging.LoggerAdapter],
            **kwargs: Any,
    ) -> _SyncOrAsyncResult: ...


ResourceSpawningFn = Union[ResourceDaemonFn, ResourceTimerFn]


class WhenFilterFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            *args: Any,
            type: str,
            event: Union[str, bodies.RawEvent],
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: str,
            name: str,
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            diff: diffs.Diff,
            old: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            new: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            **kwargs: Any,
    ) -> bool: ...


class MetaFilterFn(Protocol):
    def __call__(  # lgtm[py/similar-function]
            self,
            value: Optional[str],  # because it is either labels or annotations, nothing else.
            *args: Any,
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: str,
            name: str,
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            **kwargs: Any,
    ) -> bool: ...


_FnT = TypeVar('_FnT', WhenFilterFn, MetaFilterFn)


def all_(fns: Collection[_FnT]) -> _FnT:
    """
    Combine few callbacks into one::

        import kopf

        def whole_fn1(name, **_): return name.startswith('kopf-')
        def whole_fn2(spec, **_): return spec.get('field') == 'value'
        def value_fn1(value, **_): return value.startswith('some')
        def value_fn2(value, **_): return value.endswith('label')

        @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                        when=kopf.all_([whole_fn1, whole_fn2]),
                        labels={'somelabel': kopf.all_([value_fn1, value_fn2])})
        def create_fn(**_):
            pass

    The semantics is the same as for Python's built-in :func:`all`.
    """
    def all_fn(*args: Any, **kwargs: Any) -> bool:
        return all(fn(*args, **kwargs) for fn in fns)
    return all_fn


def any_(fns: Collection[_FnT]) -> _FnT:
    """
    Combine few callbacks into one::

        import kopf

        def whole_fn1(name, **_): return name.startswith('kopf-')
        def whole_fn2(spec, **_): return spec.get('field') == 'value'
        def value_fn1(value, **_): return value.startswith('some')
        def value_fn2(value, **_): return value.endswith('label')

        @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                        when=kopf.any_([whole_fn1, whole_fn2]),
                        labels={'somelabel': kopf.any_([value_fn1, value_fn2])})
        def create_fn(**_):
            pass

    The semantics is the same as for Python's built-in :func:`any`.
    """
    def any_fn(*args: Any, **kwargs: Any) -> bool:
        return any(fn(*args, **kwargs) for fn in fns)
    return any_fn
