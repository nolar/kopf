"""
All the functions to write the Kubernetes events on the Kubernetes objects.

They are used internally in the handling routine to show the progress,
and can be used directly from the handlers to add arbitrary custom events.

The events look like this:

    kubectl describe -f myres.yaml
    ...
    TODO

"""
import asyncio
import sys

from kopf import config
from kopf.k8s import events


# TODO: rename it it kopf.log()? kopf.events.log()? kopf.events.warn()?
async def event_async(obj, *, type, reason, message=''):
    """
    Issue an event for the object.
    """
    if isinstance(obj, (list, tuple)):
        for item in obj:
            await events.post_event(obj=item, type=type, reason=reason, message=message)
    else:
        await events.post_event(obj=obj, type=type, reason=reason, message=message)


# Shortcuts for the only two officially documented event types as of now.
# However, any arbitrary strings can be used as an event type to the base function.
async def info_async(obj, *, reason, message=''):
    if config.EventsConfig.events_loglevel > config.LOGLEVEL_INFO:
        return
    await event_async(obj, reason=reason, message=message, type='Normal')


async def warn_async(obj, *, reason, message=''):
    if config.EventsConfig.events_loglevel > config.LOGLEVEL_WARNING:
        return
    await event_async(obj, reason=reason, message=message, type='Warning')


async def exception_async(obj, *, reason='', message='', exc=None):
    if config.EventsConfig.events_loglevel > config.LOGLEVEL_ERROR:
        return

    if exc is None:
        _, exc, _ = sys.exc_info()
    reason = reason if reason else type(exc).__name__
    message = f'{message} {exc}' if message else f'{exc}'
    await event_async(obj, reason=reason, message=message, type='Error')


# Next 4 funcs are just synchronous interface for async event functions.
def event(obj, *, type, reason, message=''):
    asyncio.wait_for(event_async(obj, type=type, reason=reason, message=message), timeout=2)


def info(obj, *, reason, message=''):
    asyncio.wait_for(info_async(obj, reason=reason, message=message), timeout=2)


def warn(obj, *, reason, message=''):
    asyncio.wait_for(warn_async(obj, reason=reason, message=message), timeout=2)


def exception(obj, *, reason='', message='', exc=None):
    asyncio.wait_for(exception_async(obj, reason=reason, message=message, exc=exc), timeout=2)
