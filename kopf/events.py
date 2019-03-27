"""
All the functions to write the Kubernetes events on the Kubernetes objects.

They are used internally in the handling routine to show the progress,
and can be used directly from the handlers to add arbitrary custom events.

The events look like this:

    kubectl describe -f myres.yaml
    ...
    TODO

"""

import sys

import datetime
import kubernetes

from kopf.structs.hierarchies import build_object_reference


# TODO: rename it it kopf.log()? kopf.events.log()? kopf.events.warn()?
def event(obj, *, type, reason, message=''):
    """
    Issue an event for the object.
    """
    if isinstance(obj, (list, tuple)):
        for item in obj:
            event(obj, type=type, reason=reason, message=message)
        return

    now = datetime.datetime.utcnow()
    namespace = obj['metadata']['namespace']

    meta = kubernetes.client.V1ObjectMeta(
        namespace=namespace,
        generate_name='kopf-event-',
    )
    body = kubernetes.client.V1beta1Event(
        metadata=meta,

        action='Action?',
        type=type,
        reason=reason,
        note=message,
        # message=message,

        reporting_controller='kopf',
        reporting_instance='dev',
        deprecated_source=kubernetes.client.V1EventSource(component='kopf'),  # used in the "From" column in `kubectl describe`.

        regarding=build_object_reference(obj),
        # related=build_object_reference(obj),

        event_time=now.isoformat() + 'Z',  # '2019-01-28T18:25:03.000000Z'
        deprecated_first_timestamp=now.isoformat() + 'Z',  # used in the "Age" column in `kubectl describe`.
    )

    api = kubernetes.client.EventsV1beta1Api()
    api.create_namespaced_event(
        namespace=namespace,
        body=body,
    )


# Shortcuts for the only two officially documented event types as of now.
# However, any arbitrary strings can be used as an event type to the base function.
def info(obj, *, reason, message=''):
    return event(obj, reason=reason, message=message, type='Normal')


def warn(obj, *, reason, message=''):
    return event(obj, reason=reason, message=message, type='Warning')


def exception(obj, *, reason='', message='', exc=None):
    if exc is None:
        _, exc, _ = sys.exc_info()
    reason = reason if reason else type(exc).__name__
    message = f'{message} {exc}' if message else f'{exc}'
    return event(obj, reason=reason, message=message, type='Error')
