"""
Detection of the event causes, based on the resource state.

The low-level watch-events are highly limited in information on what
caused them, and they only notify that the object was changed somehow:

* ``ADDED`` for the newly created objects (or for the first-time listing).
* ``MODIFIED`` for the changes of any field, be that metainfo, spec, or status.
* ``DELETED`` for the actual deletion of the object post-factum.

The conversion of low-level *events* to high level *causes* is done by
checking the object's state and comparing it to the saved last-seen state.

This allows to track which specific fields were changed, and if are those
changes are important enough to call the handlers: e.g. the ``status`` changes
are ignored, so as some selected system fields of the ``metainfo``.

For deletion, the cause is detected when the object is just marked for deletion,
not when it is actually deleted (as the events notify): so that the handlers
could execute on the yet-existing object (and its children, if created).
"""
import dataclasses
import enum
import logging
import warnings
from typing import Any, Optional, Union, TypeVar

from kopf.structs import bodies
from kopf.structs import containers
from kopf.structs import diffs
from kopf.structs import finalizers
from kopf.structs import lastseen
from kopf.structs import patches
from kopf.structs import resources


class Activity(str, enum.Enum):
    STARTUP = 'startup'
    CLEANUP = 'cleanup'
    AUTHENTICATION = 'authentication'
    PROBE = 'probe'


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
    ACQUIRE = 'acquire'
    RELEASE = 'release'

    def __str__(self) -> str:
        return str(self.value)


# These sets are checked in few places, so we keep them centralised:
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
    Reason.ACQUIRE,
    Reason.RELEASE,
)
ALL_REASONS = HANDLER_REASONS + REACTOR_REASONS

# The human-readable names of these causes. Will be capitalised when needed.
TITLES = {
    Reason.CREATE: 'creation',
    Reason.UPDATE: 'update',
    Reason.DELETE: 'deletion',
    Reason.RESUME: 'resuming',
}


@dataclasses.dataclass
class BaseCause:
    logger: Union[logging.Logger, logging.LoggerAdapter]


@dataclasses.dataclass
class ActivityCause(BaseCause):
    activity: Activity


@dataclasses.dataclass
class ResourceCause(BaseCause):
    resource: resources.Resource
    patch: patches.Patch
    body: bodies.Body
    memo: containers.ObjectDict


@dataclasses.dataclass
class ResourceWatchingCause(ResourceCause):
    """
    The raw event received from the API.

    It is a read-only mapping with some extra properties and methods.
    """
    type: bodies.EventType
    raw: bodies.Event


@dataclasses.dataclass
class ResourceChangingCause(ResourceCause):
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
    def event(self) -> Reason:
        warnings.warn("`cause.event` is deprecated; use `cause.reason`.", DeprecationWarning)
        return self.reason

    @property
    def deleted(self) -> bool:
        """ Used to conditionally skip/select the @on.resume handlers if the object is deleted. """
        return finalizers.is_deleted(self.body)


def detect_resource_watching_cause(
        event: bodies.Event,
        **kwargs: Any,
) -> ResourceWatchingCause:
    return ResourceWatchingCause(
        raw=event,
        type=event['type'],
        body=event['object'],
        **kwargs)


def detect_resource_changing_cause(
        *,
        event: bodies.Event,
        diff: Optional[diffs.Diff] = None,
        requires_finalizer: bool = True,
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
    body = event['object']
    kwargs.update(body=body, initial=initial)
    if diff is not None:
        kwargs.update(diff=diff)

    # The object was really deleted from the cluster. But we do not care anymore.
    if event['type'] == 'DELETED':
        return ResourceChangingCause(reason=Reason.GONE, **kwargs)

    # The finalizer has been just removed. We are fully done.
    if finalizers.is_deleted(body) and not finalizers.has_finalizers(body):
        return ResourceChangingCause(reason=Reason.FREE, **kwargs)

    if finalizers.is_deleted(body):
        return ResourceChangingCause(reason=Reason.DELETE, **kwargs)

    # For a fresh new object, first block it from accidental deletions without our permission.
    # The actual handler will be called on the next call.
    # Only return this cause if the resource requires finalizers to be added.
    if requires_finalizer and not finalizers.has_finalizers(body):
        return ResourceChangingCause(reason=Reason.ACQUIRE, **kwargs)

    # Check whether or not the resource has finalizers, but doesn't require them. If this is
    # the case, then a resource may not be able to be deleted completely as finalizers may
    # not be removed by the operator under normal operation. We remove the finalizers first,
    # and any handler that should be called will be done on the next call.
    if not requires_finalizer and finalizers.has_finalizers(body):
        return ResourceChangingCause(reason=Reason.RELEASE, **kwargs)

    # For an object seen for the first time (i.e. just-created), call the creation handlers,
    # then mark the state as if it was seen when the creation has finished.
    # Creation never mixes with resuming, even if an object is detected on startup (first listing).
    if not lastseen.has_essence_stored(body):
        kwargs['initial'] = False
        return ResourceChangingCause(reason=Reason.CREATE, **kwargs)

    # Cases with no essence changes are usually ignored (NOOP). But for the not-yet-resumed objects,
    # we simulate a fake cause to invoke the resuming handlers. For cases with the essence changes,
    # the resuming handlers will be mixed-in to the regular cause handling ("cuckoo-style")
    # due to the ``initial=True`` flag on the cause, regardless of the reason.
    if not diff and initial:
        return ResourceChangingCause(reason=Reason.RESUME, **kwargs)

    # The previous step triggers one more patch operation without actual changes. Ignore it.
    # Either the last-seen state or the status field has changed.
    if not diff:
        return ResourceChangingCause(reason=Reason.NOOP, **kwargs)

    # And what is left, is the update operation on one of the useful fields of the existing object.
    return ResourceChangingCause(reason=Reason.UPDATE, **kwargs)


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
