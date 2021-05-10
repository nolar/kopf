import dataclasses
from typing import Optional, cast

from kopf.reactor import causation, handling
from kopf.structs import callbacks, dicts, diffs, filters, references


@dataclasses.dataclass
class ActivityHandler(handling.Handler):
    fn: callbacks.ActivityFn  # type clarification
    activity: Optional[causation.Activity]
    _fallback: bool = False  # non-public!

    def __str__(self) -> str:
        return f"Activity {self.id!r}"


@dataclasses.dataclass
class ResourceHandler(handling.Handler):
    selector: Optional[references.Selector]  # None is used only in sub-handlers
    labels: Optional[filters.MetaFilter]
    annotations: Optional[filters.MetaFilter]
    when: Optional[callbacks.WhenFilterFn]
    field: Optional[dicts.FieldPath]
    value: Optional[filters.ValueFilter]

    @property
    def requires_patching(self) -> bool:
        return True  # all typical handlers except several ones with overrides

    def adjust_cause(self, cause: handling.CauseT) -> handling.CauseT:
        if self.field is not None and isinstance(cause, causation.ChangingCause):
            old = dicts.resolve(cause.old, self.field, None)
            new = dicts.resolve(cause.new, self.field, None)
            diff = diffs.reduce(cause.diff, self.field)
            new_cause = dataclasses.replace(cause, old=old, new=new, diff=diff)
            return cast(handling.CauseT, new_cause)  # TODO: mypy bug?
        else:
            return cause


@dataclasses.dataclass
class WebhookHandler(ResourceHandler):
    fn: callbacks.WebhookFn  # type clarification
    reason: causation.WebhookType
    operation: Optional[str]
    persistent: Optional[bool]
    side_effects: Optional[bool]
    ignore_failures: Optional[bool]

    def __str__(self) -> str:
        return f"Webhook {self.id!r}"

    @property
    def requires_patching(self) -> bool:
        return False


@dataclasses.dataclass
class IndexingHandler(ResourceHandler):
    fn: callbacks.IndexingFn  # type clarification

    def __str__(self) -> str:
        return f"Indexer {self.id!r}"

    @property
    def requires_patching(self) -> bool:
        return False


@dataclasses.dataclass
class WatchingHandler(ResourceHandler):
    fn: callbacks.WatchingFn  # type clarification

    @property
    def requires_patching(self) -> bool:
        return False


@dataclasses.dataclass
class ChangingHandler(ResourceHandler):
    fn: callbacks.ChangingFn  # type clarification
    reason: Optional[causation.Reason]
    initial: Optional[bool]
    deleted: Optional[bool]  # used for mixed-in (initial==True) @on.resume handlers only.
    requires_finalizer: Optional[bool]
    field_needs_change: Optional[bool]  # to identify on-field/on-update with support for old=/new=.
    old: Optional[filters.ValueFilter]
    new: Optional[filters.ValueFilter]


@dataclasses.dataclass
class SpawningHandler(ResourceHandler):
    requires_finalizer: Optional[bool]
    initial_delay: Optional[float]


@dataclasses.dataclass
class DaemonHandler(SpawningHandler):
    fn: callbacks.DaemonFn  # type clarification
    cancellation_backoff: Optional[float]  # how long to wait before actual cancellation.
    cancellation_timeout: Optional[float]  # how long to wait before giving up on cancellation.
    cancellation_polling: Optional[float]  # how often to check for cancellation status.

    def __str__(self) -> str:
        return f"Daemon {self.id!r}"


@dataclasses.dataclass
class TimerHandler(SpawningHandler):
    fn: callbacks.TimerFn  # type clarification
    sharp: Optional[bool]
    idle: Optional[float]
    interval: Optional[float]

    def __str__(self) -> str:
        return f"Timer {self.id!r}"
