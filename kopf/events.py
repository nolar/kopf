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

from kopf.k8s import events


# TODO: rename it it kopf.log()? kopf.events.log()? kopf.events.warn()?
def event(obj, *, type, reason, message=''):
    """
    Issue an event for the object.
    """
    if isinstance(obj, (list, tuple)):
        for item in obj:
            events.post_event(obj=item, type=type, reason=reason, message=message)
    else:
        events.post_event(obj=obj, type=type, reason=reason, message=message)


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
