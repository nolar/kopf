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
import logging
from typing import NamedTuple, Text, Mapping, MutableMapping, Optional, Any, Union

from kopf.reactor import registries
from kopf.structs import diffs
from kopf.structs import finalizers
from kopf.structs import lastseen

# Constants for event types, to prevent a direct usage of strings, and typos.
# They are not exposed by the framework, but are used internally. See also: `kopf.on`.
CREATE = 'create'
UPDATE = 'update'
DELETE = 'delete'
RESUME = 'resume'
NOOP = 'noop'
FREE = 'free'
GONE = 'gone'
ACQUIRE = 'acquire'
RELEASE = 'release'

# These sets are checked in few places, so we keep them centralised:
# the user-facing causes (for handlers), and internally facing (so as handled).
HANDLER_CAUSES = (CREATE, UPDATE, DELETE, RESUME)
REACTOR_CAUSES = (NOOP, FREE, GONE, ACQUIRE, RELEASE)
ALL_CAUSES = HANDLER_CAUSES + REACTOR_CAUSES

# The human-readable names of these causes. Will be capitalised when needed.
TITLES = {
    CREATE: 'creation',
    UPDATE: 'update',
    DELETE: 'deletion',
    RESUME: 'resuming',
}


class Cause(NamedTuple):
    """
    The cause is what has caused the whole reaction as a chain of handlers.

    Unlike the low-level Kubernetes watch-events, the cause is aware
    of actual field changes, including multi-handler changes.
    """
    logger: Union[logging.Logger, logging.LoggerAdapter]
    resource: registries.Resource
    event: Text
    initial: bool
    body: MutableMapping
    patch: MutableMapping
    diff: Optional[diffs.Diff] = None
    old: Optional[Any] = None
    new: Optional[Any] = None


def detect_cause(
        event: Mapping,
        requires_finalizer: bool = True,
        **kwargs
) -> Cause:
    """
    Detect the cause of the event to be handled.

    This is a purely computational function with no side-effects.
    The causes are then consumed by `custom_object_handler`,
    which performs the actual handler invocation, logging, patching,
    and other side-effects.
    """
    diff = kwargs.get('diff')
    body = event['object']
    initial = event['type'] is None  # special value simulated by us in kopf.reactor.watching.

    # The object was really deleted from the cluster. But we do not care anymore.
    if event['type'] == 'DELETED':
        return Cause(
            event=GONE,
            body=body,
            initial=initial,
            **kwargs)

    # The finalizer has been just removed. We are fully done.
    if finalizers.is_deleted(body) and not finalizers.has_finalizers(body):
        return Cause(
            event=FREE,
            body=body,
            initial=initial,
            **kwargs)

    if finalizers.is_deleted(body):
        return Cause(
            event=DELETE,
            body=body,
            initial=initial,
            **kwargs)

    # For a fresh new object, first block it from accidental deletions without our permission.
    # The actual handler will be called on the next call.
    # Only return this cause if the resource requires finalizers to be added.
    if requires_finalizer and not finalizers.has_finalizers(body):
        return Cause(
            event=ACQUIRE,
            body=body,
            initial=initial,
            **kwargs)

    # Check whether or not the resource has finalizers, but doesn't require them. If this is
    # the case, then a resource may not be able to be deleted completely as finalizers may
    # not be removed by the operator under normal operation. We remove the finalizers first,
    # and any handler that should be called will be done on the next call.
    if not requires_finalizer and finalizers.has_finalizers(body):
        return Cause(
            event=RELEASE,
            body=body,
            initial=initial,
            **kwargs)

    # For an object seen for the first time (i.e. just-created), call the creation handlers,
    # then mark the state as if it was seen when the creation has finished.
    if not lastseen.has_state(body):
        return Cause(
            event=CREATE,
            body=body,
            initial=initial,
            **kwargs)

    # Cases with no state changes are usually ignored (NOOP). But for the "None" events,
    # as simulated for the initial listing, we call the resuming handlers (e.g. threads/tasks).
    if not diff and initial:
        return Cause(
            event=RESUME,
            body=body,
            initial=initial,
            **kwargs)

    # The previous step triggers one more patch operation without actual changes. Ignore it.
    # Either the last-seen state or the status field has changed.
    if not diff:
        return Cause(
            event=NOOP,
            body=body,
            initial=initial,
            **kwargs)

    # And what is left, is the update operation on one of the useful fields of the existing object.
    return Cause(
        event=UPDATE,
        body=body,
        initial=initial,
        **kwargs)


def enrich_cause(
        cause: Cause,
        **kwargs
) -> Cause:
    """
    Produce a new derived cause with some fields modified ().

    Usually, those are the old/new/diff fields, and used when a field-handler
    is invoked (the old/new/diff refer to the field's values only).
    """
    return cause._replace(**kwargs)
