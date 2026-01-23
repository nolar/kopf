"""
Callback signatures for typing.

Since these signatures contain a lot of copy-pasted kwargs and are
not so important for the codebase, they are moved to this separate module.

As a rule of thumb, for every kwarg named ``whatever``, there should be
a corresponding type or class ``kopf.Whatever`` with all the typing tricks
(unions, optionals, partial ``Any`` values, etc) included.
"""
import datetime
from collections.abc import Callable, Collection
from typing import TYPE_CHECKING, Any, TypeVar

from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, diffs, ephemera, patches, references, reviews
from kopf._core.actions import invocation
from kopf._core.intents import stoppers

if not TYPE_CHECKING:  # pragma: nocover
    # Define unspecified protocols for the runtime annotations -- to avoid "quoting".
    ActivityFn = Callable[..., invocation.SyncOrAsync[object | None]]
    IndexingFn = Callable[..., invocation.SyncOrAsync[object | None]]
    WatchingFn = Callable[..., invocation.SyncOrAsync[object | None]]
    ChangingFn = Callable[..., invocation.SyncOrAsync[object | None]]
    WebhookFn = Callable[..., invocation.SyncOrAsync[object | None]]
    DaemonFn = Callable[..., invocation.SyncOrAsync[object | None]]
    TimerFn = Callable[..., invocation.SyncOrAsync[object | None]]
    DelayFn = Callable[..., float]  # strictly sync, no async!
    WhenFilterFn = Callable[..., bool]  # strictly sync, no async!
    MetaFilterFn = Callable[..., bool]  # strictly sync, no async!
else:
    from mypy_extensions import Arg, DefaultNamedArg, KwArg, NamedArg

    # TODO:1: Split to specialised LoginFn, ProbeFn, StartupFn, etc. -- with different result types.
    # TODO:2: Try using ParamSpec to support index type checking in callbacks
    #         when PEP 612 is released (https://www.python.org/dev/peps/pep-0612/)
    ActivityFn = Callable[
        [
            NamedArg(configuration.OperatorSettings, "settings"),
            NamedArg(ephemera.Index[Any, Any], "*"),
            NamedArg(int, "retry"),
            NamedArg(datetime.datetime, "started"),
            NamedArg(datetime.timedelta, "runtime"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        invocation.SyncOrAsync[object | None]
    ]

    IndexingFn = Callable[
        [
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(str | None, "uid"),
            NamedArg(str | None, "name"),
            NamedArg(str | None, "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        invocation.SyncOrAsync[object | None]
    ]

    WatchingFn = Callable[
        [
            NamedArg(str, "type"),
            NamedArg(bodies.RawEvent, "event"),
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(str | None, "uid"),
            NamedArg(str | None, "name"),
            NamedArg(str | None, "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        invocation.SyncOrAsync[object | None]
    ]

    ChangingFn = Callable[
        [
            NamedArg(int, "retry"),
            NamedArg(datetime.datetime, "started"),
            NamedArg(datetime.timedelta, "runtime"),
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(str | None, "uid"),
            NamedArg(str | None, "name"),
            NamedArg(str | None, "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(str, "reason"),
            NamedArg(diffs.Diff, "diff"),
            NamedArg(bodies.BodyEssence | Any | None, "old"),
            NamedArg(bodies.BodyEssence | Any | None, "new"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        invocation.SyncOrAsync[object | None]
    ]

    WebhookFn = Callable[
        [
            NamedArg(bool, "dryrun"),
            NamedArg(list[str], "warnings"),  # mutable!
            NamedArg(str | None, "subresource"),
            NamedArg(reviews.UserInfo, "userinfo"),
            NamedArg(reviews.SSLPeer, "sslpeer"),
            NamedArg(reviews.Headers, "headers"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(str | None, "uid"),
            NamedArg(str | None, "name"),
            NamedArg(str | None, "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        invocation.SyncOrAsync[object | None]
    ]

    DaemonFn = Callable[
        [
            NamedArg(stoppers.DaemonStopped, "stopped"),
            NamedArg(int, "retry"),
            NamedArg(datetime.datetime, "started"),
            NamedArg(datetime.timedelta, "runtime"),
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(str | None, "uid"),
            NamedArg(str | None, "name"),
            NamedArg(str | None, "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        invocation.SyncOrAsync[object | None]
    ]

    TimerFn = Callable[
        [
            NamedArg(ephemera.Index[Any, Any], "*"),
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(str | None, "uid"),
            NamedArg(str | None, "name"),
            NamedArg(str | None, "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        invocation.SyncOrAsync[object | None]
    ]

    DelayFn = Callable[
        [
            NamedArg(ephemera.Index[Any, Any], "*"),
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(str | None, "uid"),
            NamedArg(str | None, "name"),
            NamedArg(str | None, "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        float  # strictly sync, no async!
    ]

    WhenFilterFn = Callable[
        [
            NamedArg(str, "type"),
            NamedArg(bodies.RawEvent, "event"),
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(str | None, "uid"),
            NamedArg(str | None, "name"),
            NamedArg(str | None, "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(diffs.Diff, "diff"),
            NamedArg(bodies.BodyEssence | Any | None, "old"),
            NamedArg(bodies.BodyEssence | Any | None, "new"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        bool  # strictly sync, no async!
    ]

    MetaFilterFn = Callable[
        [
            Arg(Any, "value"),
            NamedArg(str, "type"),
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(str | None, "uid"),
            NamedArg(str | None, "name"),
            NamedArg(str | None, "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(typedefs.Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        bool  # strictly sync, no async!
    ]

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
