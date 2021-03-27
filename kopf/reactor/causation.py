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
import logging
from typing import Any, List, Mapping, Optional, TypeVar, Union

from kopf.storage import finalizers
from kopf.structs import bodies, configuration, diffs, ephemera, handlers, \
                         ids, patches, primitives, references, reviews


@dataclasses.dataclass
class BaseCause:
    indices: ephemera.Indices
    logger: Union[logging.Logger, logging.LoggerAdapter]
    memo: ephemera.AnyMemo


@dataclasses.dataclass
class ActivityCause(BaseCause):
    activity: handlers.Activity
    settings: configuration.OperatorSettings


@dataclasses.dataclass
class ResourceCause(BaseCause):
    resource: references.Resource
    patch: patches.Patch
    body: bodies.Body


@dataclasses.dataclass
class ResourceWebhookCause(ResourceCause):
    dryrun: bool
    reason: Optional[handlers.WebhookType]  # None means "all" or expects the webhook id
    webhook: Optional[ids.HandlerId]  # None means "all"
    headers: Mapping[str, str]
    sslpeer: Mapping[str, Any]
    userinfo: reviews.UserInfo
    warnings: List[str]  # mutable!
    operation: Optional[reviews.Operation]  # None if not provided for some reason


@dataclasses.dataclass
class ResourceIndexingCause(ResourceCause):
    """
    The raw event received from the API.
    """
    pass


@dataclasses.dataclass
class ResourceWatchingCause(ResourceCause):
    """
    The raw event received from the API.

    It is a read-only mapping with some extra properties and methods.
    """
    type: bodies.RawEventType
    raw: bodies.RawEvent


@dataclasses.dataclass
class ResourceSpawningCause(ResourceCause):
    """
    An internal daemon is spawning: tasks, threads, timers.

    Used only on the first appearance of a resource as a container for resource-
    specific objects (loggers, etc).
    """
    reset: bool


@dataclasses.dataclass
class ResourceChangingCause(ResourceCause):
    """
    The cause is what has caused the whole reaction as a chain of handlers.

    Unlike the low-level Kubernetes watch-events, the cause is aware
    of actual field changes, including multi-handler changes.
    """
    initial: bool
    reason: handlers.Reason
    diff: diffs.Diff = diffs.EMPTY
    old: Optional[bodies.BodyEssence] = None
    new: Optional[bodies.BodyEssence] = None

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
    stopper: primitives.DaemonStopper  # a signaller for the termination and its reason.


def detect_resource_watching_cause(
        raw_event: bodies.RawEvent,
        body: bodies.Body,
        **kwargs: Any,
) -> ResourceWatchingCause:
    return ResourceWatchingCause(
        raw=raw_event,
        type=raw_event['type'],
        body=body,
        **kwargs)


def detect_resource_spawning_cause(
        body: bodies.Body,
        **kwargs: Any,
) -> ResourceSpawningCause:
    return ResourceSpawningCause(
        body=body,
        **kwargs)


def detect_resource_changing_cause(
        *,
        finalizer: str,
        raw_event: bodies.RawEvent,
        body: bodies.Body,
        old: Optional[bodies.BodyEssence] = None,
        new: Optional[bodies.BodyEssence] = None,
        diff: Optional[diffs.Diff] = None,
        initial: bool = False,
        **kwargs: Any,
) -> ResourceChangingCause:
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
        return ResourceChangingCause(reason=handlers.Reason.GONE, **kwargs)

    # The finalizer has been just removed. We are fully done.
    deletion_is_ongoing = finalizers.is_deletion_ongoing(body=body)
    deletion_is_blocked = finalizers.is_deletion_blocked(body=body, finalizer=finalizer)
    if deletion_is_ongoing and not deletion_is_blocked:
        return ResourceChangingCause(reason=handlers.Reason.FREE, **kwargs)

    if deletion_is_ongoing:
        return ResourceChangingCause(reason=handlers.Reason.DELETE, **kwargs)

    # For an object seen for the first time (i.e. just-created), call the creation handlers,
    # then mark the state as if it was seen when the creation has finished.
    # Creation never mixes with resuming, even if an object is detected on startup (first listing).
    if old is None:  # i.e. we have no essence stored
        kwargs['initial'] = False
        return ResourceChangingCause(reason=handlers.Reason.CREATE, **kwargs)

    # Cases with no essence changes are usually ignored (NOOP). But for the not-yet-resumed objects,
    # we simulate a fake cause to invoke the resuming handlers. For cases with the essence changes,
    # the resuming handlers will be mixed-in to the regular cause handling ("cuckoo-style")
    # due to the ``initial=True`` flag on the cause, regardless of the reason.
    if not diff and initial:
        return ResourceChangingCause(reason=handlers.Reason.RESUME, **kwargs)

    # The previous step triggers one more patch operation without actual changes. Ignore it.
    # Either the last-seen state or the status field has changed.
    if not diff:
        return ResourceChangingCause(reason=handlers.Reason.NOOP, **kwargs)

    # And what is left, is the update operation on one of the useful fields of the existing object.
    return ResourceChangingCause(reason=handlers.Reason.UPDATE, **kwargs)


_CT = TypeVar('_CT', bound=BaseCause)


def enrich_cause(
        cause: _CT,
        **kwargs: Any,
) -> _CT:
    """
    Produce a new derived cause with some fields modified ().

    Usually, those are the old/new/diff fields, and used when a field-handler
    is invoked (the old/new/diff refer to the field's values only).
    """
    return dataclasses.replace(cause, **kwargs)
