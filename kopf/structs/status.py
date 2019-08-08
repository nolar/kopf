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
                    success: abcdef1234567890fedcba
                handler2:
                    started: 2018-12-31T23:59:59,999999
                    stopped: 2018-01-01T12:34:56,789000
                    failure: abcdef1234567890fedcba
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

* ``status.kopf.progress`` stores the state of each individual handler in the
  current handling cycle (by their ids, which are usually the function names).

For each handler's status, the following is stored:

* ``started``: when the handler was attempted for the first time (used for timeouts & durations).
* ``stopped``: when the handler has failed or succeeded.
* ``delayed``: when the handler can retry again.
* ``retries``: the number of retried attempted so far (including reruns and successful attempts).
* ``success``: a digest of where the handler has succeeded (and thus no retries are needed).
* ``failure``: a digest of where the handler has failed completely (no retries will be done).
* ``message``: a brief error message from the last exception (as a hint).

When the full event cycle is executed (possibly including multiple re-runs),
the whole ``status.kopf`` section is purged. The life-long persistence of status
is not intended: otherwise, multiple distinct causes will clutter the status
and collide with each other (especially critical for multiple updates).

The digest of each handler's success or failure can be considered a "version"
of an object being handled, as it was when the handler has finished.
If the object is changed during the remaining handling cycle, the digest
of the finished handlers will be mismatching the actual digest of the object,
and so they will be re-executed.

This is conceptually close to *reconciliation*: the handling is finished
only when all handlers are executed on the latest state of the object.

Creation is treated specially: the creation handlers will never be re-executed.
In case of changes during the creation handling, the remaining creation handlers
will get the new state (as normally), and then there will be an update cycle
with all the changes since the first creation handler -- i.e. not from the last
handler as usually, when the last-seen state is updated.

Update handlers are assumed to be idempotent by concept, so it should be safe
to call them with the changes that are already reflected in the system by some
of the creation handlers.

Note: The Kubernetes-provided "resource version" of the object is not used,
as it increases with every change of the object, while this digest is used
only for the changes relevant to the operator and framework (see `get_state`).
"""

import collections.abc
import copy
import datetime


def is_started(*, body, handler):
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    return handler.id in progress


def is_sleeping(*, body, digest, handler):
    ts = get_awake_time(body=body, handler=handler)
    finished = is_finished(body=body, digest=digest, handler=handler)
    return not finished and ts is not None and ts > datetime.datetime.utcnow()


def is_awakened(*, body, digest, handler):
    finished = is_finished(body=body, digest=digest, handler=handler)
    sleeping = is_sleeping(body=body, digest=digest, handler=handler)
    return bool(not finished and not sleeping)


def is_finished(*, body, digest, handler):
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    success = progress.get(handler.id, {}).get('success', None)
    failure = progress.get(handler.id, {}).get('failure', None)
    return ((success is not None and (success is True or success == digest)) or
            (failure is not None and (failure is True or failure == digest)))


def get_start_time(*, body, patch, handler):
    progress = patch.get('status', {}).get('kopf', {}).get('progress', {})
    new_value = progress.get(handler.id, {}).get('started', None)
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    old_value = progress.get(handler.id, {}).get('started', None)
    value = new_value or old_value
    return None if value is None else datetime.datetime.fromisoformat(value)


def get_awake_time(*, body, handler):
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    value = progress.get(handler.id, {}).get('delayed', None)
    return None if value is None else datetime.datetime.fromisoformat(value)


def get_retry_count(*, body, handler):
    progress = body.get('status', {}).get('kopf', {}).get('progress', {})
    return progress.get(handler.id, {}).get('retries', None) or 0


def set_start_time(*, body, patch, handler):
    progress = patch.setdefault('status', {}).setdefault('kopf', {}).setdefault('progress', {})
    progress.setdefault(handler.id, {}).update({
        'started': datetime.datetime.utcnow().isoformat(),
    })


def set_awake_time(*, body, patch, handler, delay=None):
    if delay is not None:
        ts = datetime.datetime.utcnow() + datetime.timedelta(seconds=delay)
        ts = ts.isoformat()
    else:
        ts = None
    progress = patch.setdefault('status', {}).setdefault('kopf', {}).setdefault('progress', {})
    progress.setdefault(handler.id, {}).update({
        'delayed': ts,
    })


def set_retry_time(*, body, patch, handler, delay=None):
    retry = get_retry_count(body=body, handler=handler)
    progress = patch.setdefault('status', {}).setdefault('kopf', {}).setdefault('progress', {})
    progress.setdefault(handler.id, {}).update({
        'retries': retry + 1,
    })
    set_awake_time(body=body, patch=patch, handler=handler, delay=delay)


def store_failure(*, body, patch, digest, handler, exc):
    retry = get_retry_count(body=body, handler=handler)
    progress = patch.setdefault('status', {}).setdefault('kopf', {}).setdefault('progress', {})
    progress.setdefault(handler.id, {}).update({
        'stopped': datetime.datetime.utcnow().isoformat(),
        'failure': digest,
        'retries': retry + 1,
        'message': f'{exc}',
    })


def store_success(*, body, patch, digest, handler, result=None):
    retry = get_retry_count(body=body, handler=handler)
    progress = patch.setdefault('status', {}).setdefault('kopf', {}).setdefault('progress', {})
    progress.setdefault(handler.id, {}).update({
        'stopped': datetime.datetime.utcnow().isoformat(),
        'success': digest,
        'retries': retry + 1,
        'message': None,
    })
    store_result(patch=patch, handler=handler, result=result)


def store_result(*, patch, handler, result=None):
    if result is None:
        pass
    elif isinstance(result, collections.abc.Mapping):
        # TODO: merge recursively (patch-merge), do not overwrite the keys if they are present.
        patch.setdefault('status', {}).setdefault(handler.id, {}).update(result)
    else:
        # TODO? Fail if already present?
        patch.setdefault('status', {})[handler.id] = copy.deepcopy(result)


def purge_progress(*, body, patch):
    patch.setdefault('status', {}).setdefault('kopf', {})['progress'] = None
