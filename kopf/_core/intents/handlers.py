import dataclasses
import warnings
from collections.abc import Collection

from kopf._cogs.structs import dicts, diffs, references
from kopf._core.actions import execution
from kopf._core.intents import callbacks, causes, filters


@dataclasses.dataclass(frozen=True)
class ActivityHandler(execution.Handler):
    fn: callbacks.ActivityFn  # typing clarification
    activity: causes.Activity | None
    _fallback: bool = False  # non-public!

    def __str__(self) -> str:
        return f"Activity {self.id!r}"


@dataclasses.dataclass(frozen=True)
class ResourceHandler(execution.Handler):
    selector: references.Selector | None  # None is used only in sub-handlers
    labels: filters.MetaFilter | None
    annotations: filters.MetaFilter | None
    when: callbacks.WhenFilterFn | None
    field: dicts.FieldPath | None
    value: filters.ValueFilter | None

    def adjust_cause(self, cause: execution.CauseT) -> execution.CauseT:
        if self.field is not None and isinstance(cause, causes.ChangingCause):
            old = dicts.resolve(cause.old, self.field, None)
            new = dicts.resolve(cause.new, self.field, None)
            diff = diffs.reduce(cause.diff, self.field)
            return dataclasses.replace(cause, old=old, new=new, diff=diff)
        else:
            return cause


@dataclasses.dataclass(frozen=True)
class WebhookHandler(ResourceHandler):
    fn: callbacks.WebhookFn  # typing clarification
    reason: causes.WebhookType
    operations: Collection[str] | None
    subresource: str | None
    persistent: bool | None
    side_effects: bool | None
    ignore_failures: bool | None

    def __str__(self) -> str:
        return f"Webhook {self.id!r}"

    @property
    def operation(self) -> str | None:  # deprecated
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
    reason: causes.Reason | None
    initial: bool | None
    deleted: bool | None  # used for mixed-in (initial==True) @on.resume handlers only.
    requires_finalizer: bool | None
    field_needs_change: bool | None  # to identify on-field/on-update with support for old=/new=.
    old: filters.ValueFilter | None
    new: filters.ValueFilter | None


@dataclasses.dataclass(frozen=True)
class SpawningHandler(ResourceHandler):
    requires_finalizer: bool | None
    initial_delay: float | callbacks.DelayFn | None


@dataclasses.dataclass(frozen=True)
class DaemonHandler(SpawningHandler):
    fn: callbacks.DaemonFn  # typing clarification
    cancellation_backoff: float | None  # how long to wait before actual cancellation.
    cancellation_timeout: float | None  # how long to wait before giving up on cancellation.
    cancellation_polling: float | None  # how often to check for cancellation status.

    def __str__(self) -> str:
        return f"Daemon {self.id!r}"


@dataclasses.dataclass(frozen=True)
class TimerHandler(SpawningHandler):
    fn: callbacks.TimerFn  # typing clarification
    sharp: bool | None
    idle: float | None
    interval: float | None

    def __str__(self) -> str:
        return f"Timer {self.id!r}"
