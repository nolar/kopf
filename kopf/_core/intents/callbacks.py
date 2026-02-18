"""
Callback signatures for typing.

Since these signatures contain a lot of copy-pasted kwargs and are
not so important for the codebase, they are moved to this separate module.

As a rule of thumb, for every kwarg named ``whatever``, there should be
a corresponding type or class ``kopf.Whatever`` with all the typing tricks
(unions, optionals, partial ``Any`` values, etc) included.
"""
import datetime
from collections.abc import Collection
from typing import Any, Protocol, TypeVar

from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, diffs, ephemera, patches, references, reviews
from kopf._core.actions import invocation
from kopf._core.intents import stoppers


# TODO:1: Split to specialised LoginFn, ProbeFn, StartupFn, etc. -- with different result types.
# TODO:2: Try using ParamSpec to support index type checking in callbacks
#         when PEP 612 is released (https://www.python.org/dev/peps/pep-0612/)
class ActivityFn(Protocol):
    def __call__(
        self,
        *,
        settings: configuration.OperatorSettings,
        index: ephemera.Index[Any, Any],
        retry: int,
        started: datetime.datetime,
        runtime: datetime.timedelta,
        logger: typedefs.Logger,
        memo: Any,
        param: Any = ...,
        **kwargs: Any,
    ) -> invocation.SyncOrAsync[object | None]: ...

class IndexingFn(Protocol):
    def __call__(
        self,
        *,
        annotations: bodies.Annotations,
        labels: bodies.Labels,
        body: bodies.Body,
        meta: bodies.Meta,
        spec: bodies.Spec,
        status: bodies.Status,
        resource: references.Resource,
        uid: str | None,
        name: str | None,
        namespace: str | None,
        patch: patches.Patch,
        logger: typedefs.Logger,
        memo: Any,
        param: Any = ...,
        **kwargs: Any,
    ) -> invocation.SyncOrAsync[object | None]: ...

class WatchingFn(Protocol):
    def __call__(
        self,
        *,
        type: str,
        event: bodies.RawEvent,
        annotations: bodies.Annotations,
        labels: bodies.Labels,
        body: bodies.Body,
        meta: bodies.Meta,
        spec: bodies.Spec,
        status: bodies.Status,
        resource: references.Resource,
        uid: str | None,
        name: str | None,
        namespace: str | None,
        patch: patches.Patch,
        logger: typedefs.Logger,
        memo: Any,
        param: Any = ...,
        **kwargs: Any,
    ) -> invocation.SyncOrAsync[object | None]: ...

class ChangingFn(Protocol):
    def __call__(
        self,
        *,
        retry: int,
        started: datetime.datetime,
        runtime: datetime.timedelta,
        annotations: bodies.Annotations,
        labels: bodies.Labels,
        body: bodies.Body,
        meta: bodies.Meta,
        spec: bodies.Spec,
        status: bodies.Status,
        resource: references.Resource,
        uid: str | None,
        name: str | None,
        namespace: str | None,
        patch: patches.Patch,
        reason: str,
        diff: diffs.Diff,
        old: bodies.BodyEssence | Any | None,
        new: bodies.BodyEssence | Any | None,
        logger: typedefs.Logger,
        memo: Any,
        param: Any = ...,
        **kwargs: Any,
    ) -> invocation.SyncOrAsync[object | None]: ...

class WebhookFn(Protocol):
    def __call__(
        self,
        *,
        dryrun: bool,
        warnings: list[str],  # mutable!
        subresource: str | None,
        userinfo: reviews.UserInfo,
        sslpeer: reviews.SSLPeer,
        headers: reviews.Headers,
        labels: bodies.Labels,
        annotations: bodies.Annotations,
        body: bodies.Body,
        meta: bodies.Meta,
        spec: bodies.Spec,
        status: bodies.Status,
        resource: references.Resource,
        uid: str | None,
        name: str | None,
        namespace: str | None,
        patch: patches.Patch,
        logger: typedefs.Logger,
        memo: Any,
        param: Any = ...,
        **kwargs: Any,
    ) -> invocation.SyncOrAsync[object | None]: ...

class DaemonFn(Protocol):
    def __call__(
        self,
        *,
        stopped: stoppers.DaemonStopped,
        retry: int,
        started: datetime.datetime,
        runtime: datetime.timedelta,
        annotations: bodies.Annotations,
        labels: bodies.Labels,
        body: bodies.Body,
        meta: bodies.Meta,
        spec: bodies.Spec,
        status: bodies.Status,
        resource: references.Resource,
        uid: str | None,
        name: str | None,
        namespace: str | None,
        patch: patches.Patch,
        logger: typedefs.Logger,
        memo: Any,
        param: Any = ...,
        **kwargs: Any,
    ) -> invocation.SyncOrAsync[object | None]: ...

class TimerFn(Protocol):
    def __call__(
        self,
        *,
        index: ephemera.Index[Any, Any],
        annotations: bodies.Annotations,
        labels: bodies.Labels,
        body: bodies.Body,
        meta: bodies.Meta,
        spec: bodies.Spec,
        status: bodies.Status,
        resource: references.Resource,
        uid: str | None,
        name: str | None,
        namespace: str | None,
        patch: patches.Patch,
        logger: typedefs.Logger,
        memo: Any,
        param: Any = ...,
        **kwargs: Any,
    ) -> invocation.SyncOrAsync[object | None]: ...

class WhenFilterFn(Protocol):
    def __call__(
        self,
        *,
        type: str,
        event: bodies.RawEvent,
        annotations: bodies.Annotations,
        labels: bodies.Labels,
        body: bodies.Body,
        meta: bodies.Meta,
        spec: bodies.Spec,
        status: bodies.Status,
        resource: references.Resource,
        uid: str | None,
        name: str | None,
        namespace: str | None,
        patch: patches.Patch,
        diff: diffs.Diff,
        old: bodies.BodyEssence | Any | None,
        new: bodies.BodyEssence | Any | None,
        logger: typedefs.Logger,
        memo: Any,
        param: Any = ...,
        **kwargs: Any,
    ) -> bool: ...

class MetaFilterFn(Protocol):
    def __call__(
        self,
        value: Any,
        *,
        type: str,
        annotations: bodies.Annotations,
        labels: bodies.Labels,
        body: bodies.Body,
        meta: bodies.Meta,
        spec: bodies.Spec,
        status: bodies.Status,
        resource: references.Resource,
        uid: str | None,
        name: str | None,
        namespace: str | None,
        patch: patches.Patch,
        logger: typedefs.Logger,
        memo: Any,
        param: Any = ...,
        **kwargs: Any,
    ) -> bool: ...

SpawningFn = DaemonFn | TimerFn
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
