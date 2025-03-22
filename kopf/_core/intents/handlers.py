import dataclasses
import warnings
from typing import Collection, Optional, cast

from kopf._cogs.structs import dicts, diffs, references
from kopf._core.actions import execution
from kopf._core.intents import callbacks, causes, filters


@dataclasses.dataclass(frozen=True)
class ActivityHandler(execution.Handler):
    fn: callbacks.ActivityFn  # typing clarification
    activity: Optional[causes.Activity]
    _fallback: bool = False  # non-public!

    def __str__(self) -> str:
        return f"Activity {self.id!r}"


@dataclasses.dataclass(frozen=True)
class ResourceHandler(execution.Handler):
    selector: Optional[references.Selector]  # None is used only in sub-handlers
    labels: Optional[filters.MetaFilter]
    annotations: Optional[filters.MetaFilter]
    when: Optional[callbacks.WhenFilterFn]
    field: Optional[dicts.FieldPath]
    value: Optional[filters.ValueFilter]

    def adjust_cause(self, cause: execution.CauseT) -> execution.CauseT:
        if self.field is not None and isinstance(cause, causes.ChangingCause):
            old = dicts.resolve(cause.old, self.field, None)
            new = dicts.resolve(cause.new, self.field, None)
            diff = diffs.reduce(cause.diff, self.field)
            new_cause = dataclasses.replace(cause, old=old, new=new, diff=diff)
            return cast(execution.CauseT, new_cause)  # TODO: mypy bug?
        else:
            return cause


@dataclasses.dataclass(frozen=True)
class WebhookHandler(ResourceHandler):
    fn: callbacks.WebhookFn  # typing clarification
    reason: causes.WebhookType
    operations: Optional[Collection[str]]
    subresource: Optional[str]
    persistent: Optional[bool]
    side_effects: Optional[bool]
    ignore_failures: Optional[bool]

    def __str__(self) -> str:
        return f"Webhook {self.id!r}"

    @property
    def operation(self) -> Optional[str]:  # deprecated
        warnings.warn("handler.operation is deprecated, use handler.operations", DeprecationWarning)
        if not self.operations:
           return None
        elif len(self.operations) == 1:
            return list(self.operations)[0]
        else:
            raise ValueError(
                f"{len(self.operations)} operations in the handler. Use it as handler.operations."
            )


@dataclasses.dataclass(frozen=True)
class IndexingHandler(ResourceHandler):
    fn: callbacks.IndexingFn  # typing clarification

    def __str__(self) -> str:
        return f"Indexer {self.id!r}"


@dataclasses.dataclass(frozen=True)
class WatchingHandler(ResourceHandler):
    fn: callbacks.WatchingFn  # typing clarification


@dataclasses.dataclass(frozen=True)
class ChangingHandler(ResourceHandler):
    fn: callbacks.ChangingFn  # typing clarification
    reason: Optional[causes.Reason]
    initial: Optional[bool]
    deleted: Optional[bool]  # used for mixed-in (initial==True) @on.resume handlers only.
    requires_finalizer: Optional[bool]
    field_needs_change: Optional[bool]  # to identify on-field/on-update with support for old=/new=.
    old: Optional[filters.ValueFilter]
    new: Optional[filters.ValueFilter]


@dataclasses.dataclass(frozen=True)
class SpawningHandler(ResourceHandler):
    requires_finalizer: Optional[bool]
    initial_delay: Optional[float]


@dataclasses.dataclass(frozen=True)
class DaemonHandler(SpawningHandler):
    fn: callbacks.DaemonFn  # typing clarification
    cancellation_backoff: Optional[float]  # how long to wait before actual cancellation.
    cancellation_timeout: Optional[float]  # how long to wait before giving up on cancellation.
    cancellation_polling: Optional[float]  # how often to check for cancellation status.

    def __str__(self) -> str:
        return f"Daemon {self.id!r}"


@dataclasses.dataclass(frozen=True)
class TimerHandler(SpawningHandler):
    fn: callbacks.TimerFn  # typing clarification
    sharp: Optional[bool]
    idle: Optional[float]
    interval: Optional[float]

    def __str__(self) -> str:
        return f"Timer {self.id!r}"
