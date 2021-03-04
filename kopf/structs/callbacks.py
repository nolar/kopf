"""
Callback signatures for typing.

Since these signatures contain a lot of copy-pasted kwargs and are
not so important for the codebase, they are moved to this separate module.
"""
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, Collection, \
                   Coroutine, Dict, NewType, Optional, TypeVar, Union

from kopf.structs import bodies, diffs, ephemera, patches, primitives, references

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

LoggerType = Union[logging.Logger, logging.LoggerAdapter]


if TYPE_CHECKING:  # pragma: nocover
    from mypy_extensions import Arg, DefaultNamedArg, KwArg, NamedArg, VarArg

    # TODO: Try using ParamSpec to support index type checking in callbacks 
    # when PEP 612 is released (https://www.python.org/dev/peps/pep-0612/)
    ActivityFn = Callable[
        [
            NamedArg(ephemera.Index, "*"),
            NamedArg(int, "retry"),
            NamedArg(datetime, "started"),
            NamedArg(timedelta, "runtime"),

            NamedArg(LoggerType, "logger"),
            NamedArg(ephemera.AnyMemo, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsyncResult
    ]


    ResourceIndexingFn = Callable[
        [
            NamedArg(Dict[str, str], "labels"),
            NamedArg(Dict[str, str], "annotations"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),

            NamedArg(LoggerType, "logger"),
            NamedArg(ephemera.AnyMemo, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsyncResult
    ]


    ResourceWatchingFn = Callable[
        [
            NamedArg(str, "type"),
            NamedArg(bodies.RawEvent, "event"),

            NamedArg(Dict[str, str], "labels"),
            NamedArg(Dict[str, str], "annotations"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            
            NamedArg(LoggerType, "logger"),
            NamedArg(ephemera.AnyMemo, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsyncResult
    ]


    ResourceChangingFn = Callable[
        [
            NamedArg(int, "retry"),
            NamedArg(datetime, "started"),
            NamedArg(timedelta, "runtime"),

            NamedArg(Dict[str, str], "labels"),
            NamedArg(Dict[str, str], "annotations"),
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

            NamedArg(LoggerType, "logger"),
            NamedArg(ephemera.AnyMemo, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsyncResult
    ]


    ResourceDaemonFn = Callable[
        [
            NamedArg(primitives.SyncDaemonStopperChecker, "stopped"),

            NamedArg(int, "retry"),
            NamedArg(datetime, "started"),
            NamedArg(timedelta, "runtime"),

            NamedArg(Dict[str, str], "labels"),
            NamedArg(Dict[str, str], "annotations"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),
            
            NamedArg(LoggerType, "logger"),
            NamedArg(ephemera.AnyMemo, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsyncResult
    ]


    ResourceTimerFn = Callable[
        [
            NamedArg(ephemera.Index, "*"),

            NamedArg(Dict[str, str], "labels"),
            NamedArg(Dict[str, str], "annotations"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),

            NamedArg(LoggerType, "logger"),
            NamedArg(ephemera.AnyMemo, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        _SyncOrAsyncResult
    ]


    ResourceSpawningFn = Union[ResourceDaemonFn, ResourceTimerFn]

    WhenFilterFn = Callable[
        [
            NamedArg(str, "type"),
            NamedArg(bodies.RawEvent, "event"),

            NamedArg(Dict[str, str], "labels"),
            NamedArg(Dict[str, str], "annotations"),
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
            
            NamedArg(LoggerType, "logger"),
            NamedArg(ephemera.AnyMemo, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        bool
    ]


    MetaFilterFn = Callable[
        [
            Arg(Any, "value"),
            NamedArg(str, "type"),

            NamedArg(Dict[str, str], "labels"),
            NamedArg(Dict[str, str], "annotations"),
            NamedArg(bodies.Body, "body"),
            NamedArg(bodies.Meta, "meta"),
            NamedArg(bodies.Spec, "spec"),
            NamedArg(bodies.Status, "status"),
            NamedArg(references.Resource, "resource"),
            NamedArg(Optional[str], "uid"),
            NamedArg(Optional[str], "name"),
            NamedArg(Optional[str], "namespace"),
            NamedArg(patches.Patch, "patch"),

            NamedArg(LoggerType, "logger"),
            NamedArg(ephemera.AnyMemo, "memo"),
            DefaultNamedArg(Any, "param"),
            KwArg(Any),
        ],
        bool
    ]


_FnT = TypeVar('_FnT', "WhenFilterFn", "MetaFilterFn")


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
