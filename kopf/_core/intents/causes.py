"""
Detection of the event causes, based on the resource state.

The low-level watch-events are highly limited in information on what
caused them, and they only notify that the object was changed somehow:

* ``ADDED`` for the newly created objects (or for the first-time listing).
* ``MODIFIED`` for the changes of any field, be that metadata, spec, or status.
* ``DELETED`` for the actual deletion of the object post-factum.

The conversion of low-level *events* to high level *causes* is done by
checking the object's state and comparing it to the saved last-seen state.

This allows to track which specific fields were changed, and if are those
changes are important enough to call the handlers: e.g. the ``status`` changes
are ignored, so as some selected system fields of the ``metadata``.

For deletion, the cause is detected when the object is just marked for deletion,
not when it is actually deleted (as the events notify): so that the handlers
could execute on the yet-existing object (and its children, if created).
"""
import dataclasses
import enum
from typing import Any, List, Mapping, Optional

from kopf._cogs.configs import configuration
from kopf._cogs.structs import bodies, diffs, ephemera, finalizers, \
                               ids, patches, references, reviews
from kopf._core.actions import execution
from kopf._core.intents import stoppers


class Activity(str, enum.Enum):
    STARTUP = 'startup'
    CLEANUP = 'cleanup'
    AUTHENTICATION = 'authentication'
    PROBE = 'probe'


class WebhookType(str, enum.Enum):
    VALIDATING = 'validating'
    MUTATING = 'mutating'

    def __str__(self) -> str:
        return str(self.value)


# Constants for cause types, to prevent a direct usage of strings, and typos.
# They are not exposed by the framework, but are used internally. See also: `kopf.on`.
class Reason(str, enum.Enum):
    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'
    RESUME = 'resume'
    NOOP = 'noop'
    FREE = 'free'
    GONE = 'gone'

    def __str__(self) -> str:
        return str(self.value)


# These sets are checked in a few places, so we keep them centralised:
# the user-facing causes (for handlers) and internally facing (for the reactor).
HANDLER_REASONS = (
    Reason.CREATE,
    Reason.UPDATE,
    Reason.DELETE,
    Reason.RESUME,
)
REACTOR_REASONS = (
    Reason.NOOP,
    Reason.FREE,
    Reason.GONE,
)
ALL_REASONS = HANDLER_REASONS + REACTOR_REASONS

# The human-readable names of these causes. Will be capitalised when needed.
TITLES = {
    Reason.CREATE.value: 'creation',
    Reason.UPDATE.value: 'updating',
    Reason.DELETE.value: 'deletion',
    Reason.RESUME.value: 'resuming',
}


@dataclasses.dataclass
class BaseCause(execution.Cause):
    """
    Base non-specific cause as used in the framework's reactor in most cases.

    IMPORTANT! Indices overwrite any other kwargs, even the existing ones.

    Why so? Here is why: for forwards & backwards compatibility.
    If an handler uses an index named "children", and Kopf introduces
    a new kwarg "children", the handler's code could break on the upgrade.
    To prevent this, Kopf overwrites the framework's kwarg "children"
    with the operator's index "children" and lets the developers rename it
    when (and if) they want the new kwarg.

    Naming the new indices the same as the known/existing kwargs
    harms only the developers who do so, so this is considered safe.
    """
    indices: ephemera.Indices
    memo: ephemera.AnyMemo

    @property
    def _kwargs(self) -> Mapping[str, Any]:
        kwargs = dict(super()._kwargs)
        del kwargs['indices']
        return kwargs

    @property
    def _super_kwargs(self) -> Mapping[str, Any]:
        return self.indices


@dataclasses.dataclass
class ActivityCause(BaseCause):
    activity: Activity
    settings: configuration.OperatorSettings


@dataclasses.dataclass
class ResourceCause(BaseCause):
    resource: references.Resource
    patch: patches.Patch
    body: bodies.Body

    @property
    def _kwargs(self) -> Mapping[str, Any]:
        return dict(
            super()._kwargs,
            spec=self.body.spec,
            meta=self.body.metadata,
            status=self.body.status,
            uid=self.body.metadata.uid,
            name=self.body.metadata.name,
            namespace=self.body.metadata.namespace,
            labels=self.body.metadata.labels,
            annotations=self.body.metadata.annotations,
        )


@dataclasses.dataclass
class WebhookCause(ResourceCause):
    dryrun: bool
    reason: Optional[WebhookType]  # None means "all" or expects the webhook id
    webhook: Optional[ids.HandlerId]  # None means "all"
    headers: Mapping[str, str]
    sslpeer: Mapping[str, Any]
    userinfo: reviews.UserInfo
    warnings: List[str]  # mutable!
    operation: Optional[reviews.Operation]  # None if not provided for some reason
    subresource: Optional[str]  # e.g. "status", "scale"; None for the main resource body
    old: Optional[bodies.Body] = None
    new: Optional[bodies.Body] = None
    diff: Optional[diffs.Diff] = None

    @property
    def _kwargs(self) -> Mapping[str, Any]:
        kwargs = dict(super()._kwargs)
        del kwargs['reason']
        del kwargs['webhook']
        return kwargs


@dataclasses.dataclass
class IndexingCause(ResourceCause):
    """
    The raw event received from the API.
    """
    pass


@dataclasses.dataclass
class WatchingCause(ResourceCause):
    """
    The raw event received from the API.

    It is a read-only mapping with some extra properties and methods.
    """
    type: bodies.RawEventType
    event: bodies.RawEvent


@dataclasses.dataclass
class SpawningCause(ResourceCause):
    """
    An internal daemon is spawning: tasks, threads, timers.

    Used only on the first appearance of a resource as a container for resource-
    specific objects (loggers, etc).
    """
    reset: bool

    @property
    def _kwargs(self) -> Mapping[str, Any]:
        kwargs = dict(super()._kwargs)
        del kwargs['reset']
        return kwargs


@dataclasses.dataclass
class ChangingCause(ResourceCause):
    """
    The cause is what has caused the whole reaction as a chain of handlers.

    Unlike the low-level Kubernetes watch-events, the cause is aware
    of actual field changes, including multi-handler changes.
    """
    initial: bool
    reason: Reason
    diff: diffs.Diff = diffs.EMPTY
    old: Optional[bodies.BodyEssence] = None
    new: Optional[bodies.BodyEssence] = None

    @property
    def _kwargs(self) -> Mapping[str, Any]:
        kwargs = dict(super()._kwargs)
        del kwargs['initial']
        return kwargs

    @property
    def deleted(self) -> bool:
        """ Used to conditionally skip/select the @on.resume handlers if the object is deleted. """
        return finalizers.is_deletion_ongoing(self.body)


@dataclasses.dataclass
class DaemonCause(ResourceCause):
    """
    An exceptional case of a container for daemon invocation kwargs.

    Regular causes are usually short-term, triggered by a watch-stream event,
    and disappear once the event is processed. The processing includes
    daemon spawning: the original cause and its temporary watch-event
    should not be remembered though the whole life cycle of a daemon.

    Instead, a new artificial daemon-cause is used (this class), which
    passes the kwarg values to the invocation routines. It only contains
    the long-living kwargs: loggers, per-daemon stoppers, body-views
    (with only the latest bodies as contained values), etc.

    Unlike other causes, it is created not in the processing routines once
    per event, but in the daemon spawning routines once per daemon (or a timer).
    Therefore, it is not "detected", but is created directly as an instance.
    """
    stopper: stoppers.DaemonStopper  # a signaller for the termination and its reason.

    @property
    def _kwargs(self) -> Mapping[str, Any]:
        kwargs = dict(super()._kwargs)
        del kwargs['stopper']
        return kwargs

    @property
    def _sync_kwargs(self) -> Mapping[str, Any]:
        return dict(super()._sync_kwargs, stopped=self.stopper.sync_waiter)

    @property
    def _async_kwargs(self) -> Mapping[str, Any]:
        return dict(super()._async_kwargs, stopped=self.stopper.async_waiter)


def detect_watching_cause(
        raw_event: bodies.RawEvent,
        body: bodies.Body,
        **kwargs: Any,
) -> WatchingCause:
    return WatchingCause(
        event=raw_event,
        type=raw_event['type'],
        body=body,
        **kwargs)


def detect_spawning_cause(
        body: bodies.Body,
        **kwargs: Any,
) -> SpawningCause:
    return SpawningCause(
        body=body,
        **kwargs)


def detect_changing_cause(
        *,
        finalizer: str,
        raw_event: bodies.RawEvent,
        body: bodies.Body,
        old: Optional[bodies.BodyEssence] = None,
        new: Optional[bodies.BodyEssence] = None,
        diff: Optional[diffs.Diff] = None,
        initial: bool = False,
        **kwargs: Any,
) -> ChangingCause:
    """
    Detect the cause of the event to be handled.

    This is a purely computational function with no side-effects.
    The causes are then consumed by `custom_object_handler`,
    which performs the actual handler invocation, logging, patching,
    and other side-effects.
    """

    # Put them back to the pass-through kwargs (to avoid code duplication).
    kwargs.update(body=body, old=old, new=new, initial=initial)
    if diff is not None:
        kwargs.update(diff=diff)

    # The object was really deleted from the cluster. But we do not care anymore.
    if raw_event['type'] == 'DELETED':
        return ChangingCause(reason=Reason.GONE, **kwargs)

    # The finalizer has been just removed. We are fully done.
    deletion_is_ongoing = finalizers.is_deletion_ongoing(body=body)
    deletion_is_blocked = finalizers.is_deletion_blocked(body=body, finalizer=finalizer)
    if deletion_is_ongoing and not deletion_is_blocked:
        return ChangingCause(reason=Reason.FREE, **kwargs)

    if deletion_is_ongoing:
        return ChangingCause(reason=Reason.DELETE, **kwargs)

    # For an object seen for the first time (i.e. just-created), call the creation handlers,
    # then mark the state as if it was seen when the creation has finished.
    # Creation never mixes with resuming, even if an object is detected on startup (first listing).
    if old is None:  # i.e. we have no essence stored
        kwargs['initial'] = False
        return ChangingCause(reason=Reason.CREATE, **kwargs)

    # Cases with no essence changes are usually ignored (NOOP). But for the not-yet-resumed objects,
    # we simulate a fake cause to invoke the resuming handlers. For cases with the essence changes,
    # the resuming handlers will be mixed-in to the regular cause handling ("cuckoo-style")
    # due to the ``initial=True`` flag on the cause, regardless of the reason.
    if not diff and initial:
        return ChangingCause(reason=Reason.RESUME, **kwargs)

    # The previous step triggers one more patch operation without actual changes. Ignore it.
    # Either the last-seen state or the status field has changed.
    if not diff:
        return ChangingCause(reason=Reason.NOOP, **kwargs)

    # And what is left, is the update operation on one of the useful fields of the existing object.
    return ChangingCause(reason=Reason.UPDATE, **kwargs)
