import datetime
import logging

import kubernetes.client.rest

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 1024
CUT_MESSAGE_INFIX = '...'


def post_event(*, obj, type, reason, message=''):
    """
    Issue an event for the object.
    """

    now = datetime.datetime.utcnow()
    namespace = obj['metadata']['namespace']

    # Prevent a common case of event posting errors but shortening the message.
    if len(message) > MAX_MESSAGE_LENGTH:
        infix = CUT_MESSAGE_INFIX
        prefix = message[:MAX_MESSAGE_LENGTH // 2 - (len(infix) // 2)]
        suffix = message[-MAX_MESSAGE_LENGTH // 2 + (len(infix) - len(infix) // 2):]
        message = f'{prefix}{infix}{suffix}'

    # Object reference - similar to the owner reference, but different.
    ref = dict(
        apiVersion=obj['apiVersion'],
        kind=obj['kind'],
        name=obj['metadata']['name'],
        uid=obj['metadata']['uid'],
        namespace=obj['metadata']['namespace'],
    )

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

    try:
        api = kubernetes.client.CoreV1Api()
        api.create_namespaced_event(
            namespace=namespace,
            body=body,
        )
    except kubernetes.client.rest.ApiException as e:
        # Events are helpful but auxiliary, they should not fail the handling cycle.
        # Yet we want to notice that something went wrong (in logs).
        logger.warning("Failed to post an event. Ignoring and continuing. "
                       f"Error: {e!r}. "
                       f"Event: type={type!r}, reason={reason!r}, message={message!r}.")
