import asyncio
import datetime
import functools
import logging

import kubernetes.client.rest

from kopf import config
from kopf.structs import hierarchies

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 1024
CUT_MESSAGE_INFIX = '...'


async def post_event(*, obj=None, ref=None, type, reason, message=''):
    """
    Issue an event for the object.

    This is where they can also be accumulated, aggregated, grouped,
    and where the rate-limits should be maintained. It can (and should)
    be done by the client library, as it is done in the Go client.
    """

    # Object reference - similar to the owner reference, but different.
    if obj is not None and ref is not None:
        raise TypeError("Only one of obj= and ref= is allowed for a posted event. Got both.")
    if obj is None and ref is None:
        raise TypeError("One of obj= and ref= is required for a posted event. Got none.")
    if ref is None:
        ref = hierarchies.build_object_reference(obj)

    now = datetime.datetime.utcnow()
    namespace = ref['namespace'] or 'default'

    # Prevent a common case of event posting errors but shortening the message.
    if len(message) > MAX_MESSAGE_LENGTH:
        infix = CUT_MESSAGE_INFIX
        prefix = message[:MAX_MESSAGE_LENGTH // 2 - (len(infix) // 2)]
        suffix = message[-MAX_MESSAGE_LENGTH // 2 + (len(infix) - len(infix) // 2):]
        message = f'{prefix}{infix}{suffix}'

    meta = kubernetes.client.V1ObjectMeta(
        namespace=namespace,
        generate_name='kopf-event-',
    )
    body = kubernetes.client.V1Event(
        metadata=meta,

        action='Action?',
        type=type,
        reason=reason,
        message=message,

        reporting_component='kopf',
        reporting_instance='dev',
        source=kubernetes.client.V1EventSource(component='kopf'),  # used in the "From" column in `kubectl describe`.

        involved_object=ref,

        first_timestamp=now.isoformat() + 'Z',  # '2019-01-28T18:25:03.000000Z' -- seen in `kubectl describe ...`
        last_timestamp=now.isoformat() + 'Z',  # '2019-01-28T18:25:03.000000Z' - seen in `kubectl get events`
        event_time=now.isoformat() + 'Z',  # '2019-01-28T18:25:03.000000Z'
    )

    api = kubernetes.client.CoreV1Api()
    loop = asyncio.get_running_loop()

    try:
        await loop.run_in_executor(
            config.WorkersConfig.get_syn_executor(),
            functools.partial(api.create_namespaced_event, **{'namespace': namespace, 'body': body})
        )
    except kubernetes.client.rest.ApiException as e:
        # Events are helpful but auxiliary, they should not fail the handling cycle.
        # Yet we want to notice that something went wrong (in logs).
        logger.warning("Failed to post an event. Ignoring and continuing. "
                       f"Error: {e!r}. "
                       f"Event: type={type!r}, reason={reason!r}, message={message!r}.")
