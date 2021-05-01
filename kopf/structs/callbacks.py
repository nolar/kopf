"""
Callback signatures for typing.

Since these signatures contain a lot of copy-pasted kwargs and are
not so important for the codebase, they are moved to this separate module.

As a rule of thumb, for every kwarg named ``whatever``, there should be
a corresponding type or class ``kopf.Whatever`` with all the typing tricks
(``Union[...]``, ``Optional[...]``, partial ``Any`` values, etc) included.
"""
import datetime
import logging
from typing import TYPE_CHECKING, Any, Callable, Collection, \
                   Coroutine, List, NewType, Optional, TypeVar, Union

from kopf.structs import bodies, configuration, diffs, ephemera, \
                         patches, primitives, references, reviews

# As publicly exposed: we only promise that it is based on one of the built-in loggable classes.
# Mind that these classes have multi-versioned stubs, so we avoid redefining the protocol ourselves.
Logger = Union[logging.Logger, logging.LoggerAdapter]

# A specialised type to highlight the purpose or origin of the data of type Any,
# to not be mixed with other arbitrary Any values, where it is indeed "any".
Result = NewType('Result', object)

# An internal typing hack shows that the handler can be sync fn with the result,
# or an async fn which returns a coroutine which, in turn, returns the result.
# Used in some protocols only and is never exposed to other modules.
_R = TypeVar('_R')
_SyncOrAsync = Union[_R, Coroutine[None, None, _R]]

# A generic sync-or-async callable with no args/kwargs checks (unlike in protocols).
# Used for the BaseHandler and generic invocation methods (which do not care about protocols).
BaseFn = Callable[..., _SyncOrAsync[Optional[object]]]

if not TYPE_CHECKING:  # pragma: nocover
    # Define unspecified protocols for the runtime annotations -- to avoid "quoting".
    ActivityFn = Callable[..., _SyncOrAsync[Optional[object]]]
    ResourceIndexingFn = Callable[..., _SyncOrAsync[Optional[object]]]
    ResourceWatchingFn = Callable[..., _SyncOrAsync[Optional[object]]]
    ResourceChangingFn = Callable[..., _SyncOrAsync[Optional[object]]]
    ResourceWebhookFn = Callable[..., _SyncOrAsync[None]]
    ResourceDaemonFn = Callable[..., _SyncOrAsync[Optional[object]]]
    ResourceTimerFn = Callable[..., _SyncOrAsync[Optional[object]]]
    WhenFilterFn = Callable[..., bool]  # strictly sync, no async!
    MetaFilterFn = Callable[..., bool]  # strictly sync, no async!
else:
    from mypy_extensions import Arg, DefaultNamedArg, KwArg, NamedArg, VarArg

    # TODO:1: Split to specialised LoginFn, ProbeFn, StartupFn, etc. -- with different result types.
    # TODO:2: Try using ParamSpec to support index type checking in callbacks
    #         when PEP 612 is released (https://www.python.org/dev/peps/pep-0612/)
    ActivityFn = Callable[
        [
            NamedArg(configuration.OperatorSettings, "settings"),
            NamedArg(ephemera.Index, "*"),
            NamedArg(int, "retry"),
            NamedArg(datetime.datetime, "started"),
            NamedArg(datetime.timedelta, "runtime"),
            NamedArg(Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsync[Optional[object]]
    ]

    ResourceIndexingFn = Callable[
        [
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsync[Optional[object]]
    ]

    ResourceWatchingFn = Callable[
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
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsync[Optional[object]]
    ]

    ResourceChangingFn = Callable[
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
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(str, "reason"),
            NamedArg(diffs.Diff, "diff"),
            NamedArg(Optional[Union[bodies.BodyEssence, Any]], "old"),
            NamedArg(Optional[Union[bodies.BodyEssence, Any]], "new"),
            NamedArg(Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsync[Optional[object]]
    ]

    ResourceWebhookFn = Callable[
        [
            NamedArg(bool, "dryrun"),
            NamedArg(List[str], "warnings"),  # mutable!
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
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsync[None]
    ]

    ResourceDaemonFn = Callable[
        [
            NamedArg(primitives.DaemonStoppingFlag, "stopped"),
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
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsync[Optional[object]]
    ]

    ResourceTimerFn = Callable[
        [
            NamedArg(ephemera.Index, "*"),
            NamedArg(bodies.Annotations, "annotations"),
            NamedArg(bodies.Labels, "labels"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsync[Optional[object]]
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
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(diffs.Diff, "diff"),
            NamedArg(Optional[Union[bodies.BodyEssence, Any]], "old"),
            NamedArg(Optional[Union[bodies.BodyEssence, Any]], "new"),
            NamedArg(Logger, "logger"),
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
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            NamedArg(Logger, "logger"),
            NamedArg(Any, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        bool  # strictly sync, no async!
    ]

ResourceSpawningFn = Union[ResourceDaemonFn, ResourceTimerFn]
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
