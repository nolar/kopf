import copy
import datetime
import logging
from typing import Optional

import aiohttp

from kopf.clients import auth
from kopf.structs import bodies
from kopf.structs import resources

logger = logging.getLogger(__name__)

EVENTS_V1BETA1_CRD = resources.Resource('events.k8s.io', 'v1beta1', 'events')
EVENTS_CORE_V1_CRD = resources.Resource('', 'v1', 'events')

MAX_MESSAGE_LENGTH = 1024
CUT_MESSAGE_INFIX = '...'


@auth.reauthenticated_request
async def post_event(
        *,
        ref: bodies.ObjectReference,
        type: str,
        reason: str,
        message: str = '',
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> None:
    """
    Issue an event for the object.

    This is where they can also be accumulated, aggregated, grouped,
    and where the rate-limits should be maintained. It can (and should)
    be done by the client library, as it is done in the Go client.
    """
    if session is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    # See #164. For cluster-scoped objects, use the current namespace from the current context.
    # It could be "default", but in some systems, we are limited to one specific namespace only.
    namespace: str = ref.get('namespace') or session.default_namespace or 'default'
    full_ref: bodies.ObjectReference = copy.copy(ref)
    full_ref['namespace'] = namespace

    # Prevent a common case of event posting errors but shortening the message.
    if len(message) > MAX_MESSAGE_LENGTH:
        infix = CUT_MESSAGE_INFIX
        prefix = message[:MAX_MESSAGE_LENGTH // 2 - (len(infix) // 2)]
        suffix = message[-MAX_MESSAGE_LENGTH // 2 + (len(infix) - len(infix) // 2):]
        message = f'{prefix}{infix}{suffix}'

    now = datetime.datetime.utcnow()
    body = {
        'metadata': {
            'namespace': namespace,
            'generateName': 'kopf-event-',
        },

        'action': 'Action?',
        'type': type,
        'reason': reason,
        'message': message,

        'reportingComponent': 'kopf',
        'reportingInstance': 'dev',
        'source' : {'component': 'kopf'},  # used in the "From" column in `kubectl describe`.

        'involvedObject': full_ref,

        'firstTimestamp': now.isoformat() + 'Z',  # '2019-01-28T18:25:03.000000Z' -- seen in `kubectl describe ...`
        'lastTimestamp': now.isoformat() + 'Z',  # '2019-01-28T18:25:03.000000Z' - seen in `kubectl get events`
        'eventTime': now.isoformat() + 'Z',  # '2019-01-28T18:25:03.000000Z'
    }

    try:
        response = await session.post(
            url=EVENTS_CORE_V1_CRD.get_url(server=session.server, namespace=namespace),
            headers={'Content-Type': 'application/json'},
            json=body,
        )
        response.raise_for_status()

    except aiohttp.ClientResponseError as e:
        # Events are helpful but auxiliary, they should not fail the handling cycle.
        # Yet we want to notice that something went wrong (in logs).
        logger.warning("Failed to post an event. Ignoring and continuing. "
                       f"Error: {e!r}. "
                       f"Event: type={type!r}, reason={reason!r}, message={message!r}.")
