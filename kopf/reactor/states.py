"""
The routines to manipulate the handler progression over the event cycle.

Used to track which handlers are finished, which are not yet,
and how many retries were there.

There could be more than one low-level k8s watch-events per one actual
high-level kopf-event (a cause). The handlers are called at different times,
and the overall handling routine should persist the handler status somewhere.

The structure is this:

.. code-block: yaml

    metainfo: ...
    spec: ...
    status: ...
        kopf:
            progress:
                handler1:
                    started: 2018-12-31T23:59:59,999999
                    stopped: 2018-01-01T12:34:56,789000
                    success: true
                handler2:
                    started: 2018-12-31T23:59:59,999999
                    stopped: 2018-01-01T12:34:56,789000
                    failure: true
                    message: "Error message."
                handler3:
                    started: 2018-12-31T23:59:59,999999
                    retries: 30
                handler3/sub1:
                    started: 2018-12-31T23:59:59,999999
                    delayed: 2018-01-01T12:34:56,789000
                    retries: 10
                    message: "Not ready yet."
                handler3/sub2:
                    started: 2018-12-31T23:59:59,999999

* ``status.kopf.success`` are the handlers that succeeded (no re-execution).
* ``status.kopf.failure`` are the handlers that failed completely (no retries).
* ``status.kopf.delayed`` are the timestamps, until which these handlers sleep.
* ``status.kopf.retries`` are number of retries for succeeded, failed,
  and for the progressing handlers.

When the full event cycle is executed (possibly including multiple re-runs),
the whole ``status.kopf`` section is purged. The life-long persistence of status
is not intended: otherwise, multiple distinct causes will clutter the status
and collide with each other (especially critical for multiple updates).
"""

import collections.abc
import copy
import dataclasses
import datetime
from typing import Optional, Mapping

from kopf.reactor import registries
from kopf.structs import bodies
from kopf.structs import patches


@dataclasses.dataclass(frozen=True)
class HandlerOutcome:
    """
    An in-memory outcome of one single invocation of one single handler.

    Conceptually, an outcome is similar to the async futures, but some cases
    are handled specially: e.g., the temporary errors have exceptions,
    but the handler should be retried later, unlike with the permanent errors.
    """
    final: bool
    delay: Optional[float] = None
    result: Optional[registries.HandlerResult] = None
    exception: Optional[Exception] = None


def is_started(
        *,
        body: bodies.Body,
        handler: registries.ResourceHandler,
) -> bool:
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    return handler.id in progress


def is_sleeping(
        *,
        body: bodies.Body,
        handler: registries.ResourceHandler,
) -> bool:
    ts = get_awake_time(body=body, handler=handler)
    finished = is_finished(body=body, handler=handler)
    return not finished and ts is not None and ts > datetime.datetime.utcnow()


def is_awakened(
        *,
        body: bodies.Body,
        handler: registries.ResourceHandler,
) -> bool:
    finished = is_finished(body=body, handler=handler)
    sleeping = is_sleeping(body=body, handler=handler)
    return bool(not finished and not sleeping)


def is_finished(
        *,
        body: bodies.Body,
        handler: registries.ResourceHandler,
) -> bool:
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    success = progress.get(handler.id, {}).get('success', None)
    failure = progress.get(handler.id, {}).get('failure', None)
    return bool(success or failure)


def get_start_time(
        *,
        body: bodies.Body,
        patch: patches.Patch,
        handler: registries.ResourceHandler,
) -> Optional[datetime.datetime]:
    progress = patch.get('status', {}).get('kopf', {}).get('progress', {})
    new_value = progress.get(handler.id, {}).get('started', None)
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    old_value = progress.get(handler.id, {}).get('started', None)
    value = new_value or old_value
    return None if value is None else datetime.datetime.fromisoformat(value)


def get_awake_time(
        *,
        body: bodies.Body,
        handler: registries.ResourceHandler,
) -> Optional[datetime.datetime]:
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    value = progress.get(handler.id, {}).get('delayed', None)
    return None if value is None else datetime.datetime.fromisoformat(value)


def get_retry_count(
        *,
        body: bodies.Body,
        handler: registries.ResourceHandler,
) -> int:
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    return progress.get(handler.id, {}).get('retries', None) or 0


def set_start_time(
        *,
        body: bodies.Body,
        patch: patches.Patch,
        handler: registries.ResourceHandler,
) -> None:
    progress = patch.setdefault('status', {}).setdefault('kopf', {}).setdefault('progress', {})
    progress.setdefault(handler.id, {}).update({
        'started': datetime.datetime.utcnow().isoformat(),
    })


def persist_progress(
        *,
        outcomes: Mapping[registries.HandlerId, HandlerOutcome],
        patch: patches.Patch,
        body: bodies.Body,
) -> None:
    current = body.get('status', {}).get('kopf', {}).get('progress', {})
    storage = patch.setdefault('status', {}).setdefault('kopf', {}).setdefault('progress', {})
    for handler_id, outcome in outcomes.items():
        retry = current.get(handler_id, {}).get('retries', None) or 0
        ts_str: Optional[str]
        if outcome.delay is not None:
            ts = datetime.datetime.utcnow() + datetime.timedelta(seconds=outcome.delay)
            ts_str = ts.isoformat()
        else:
            ts_str = None

        if not outcome.final:
            storage.setdefault(handler_id, {}).update({
                'delayed': ts_str,
                'retries': retry + 1,
            })
        elif outcome.exception is not None:
            storage.setdefault(handler_id, {}).update({
                'stopped': datetime.datetime.utcnow().isoformat(),
                'failure': True,
                'retries': retry + 1,
                'message': f'{outcome.exception}',
            })
        else:
            storage.setdefault(handler_id, {}).update({
                'stopped': datetime.datetime.utcnow().isoformat(),
                'success': True,
                'retries': retry + 1,
                'message': None,
            })


def deliver_results(
        *,
        outcomes: Mapping[registries.HandlerId, HandlerOutcome],
        patch: patches.Patch,
) -> None:
    """
    Store the results (as returned from the handlers) to the resource.

    This is not the handlers' state persistence, but the results' persistence.

    First, the state persistence is stored under ``.status.kopf.progress``,
    and can (later) be configured to be stored in different fields for different
    operators operating the same objects: ``.status.kopf.{somename}.progress``.
    The handlers' result are stored in the top-level ``.status``.

    Second, the handler results can (also later) be delivered to other objects,
    e.g. to their owners or label-selected related objects. For this, another
    class/module will be added.

    For now, we keep state- and result persistence in one module, but separated.
    """
    for handler_id, outcome in outcomes.items():
        if outcome.exception is not None:
            pass
        elif outcome.result is None:
            pass
        elif isinstance(outcome.result, collections.abc.Mapping):
            # TODO: merge recursively (patch-merge), do not overwrite the keys if they are present.
            patch.setdefault('status', {}).setdefault(handler_id, {}).update(outcome.result)
        else:
            patch.setdefault('status', {})[handler_id] = copy.deepcopy(outcome.result)


def purge_progress(
        *,
        body: bodies.Body,
        patch: patches.Patch,
) -> None:
    patch.setdefault('status', {}).setdefault('kopf', {})['progress'] = None
