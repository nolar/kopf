import copy
import datetime

import aiohttp

from kopf._cogs.clients import api, errors
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, references

MAX_MESSAGE_LENGTH = 1024
CUT_MESSAGE_INFIX = '...'


async def post_event(
        *,
        ref: bodies.ObjectReference,
        type: str,
        reason: str,
        message: str = '',
        resource: references.Resource,
        settings: configuration.OperatorSettings,
        logger: typedefs.Logger,
) -> None:
    """
    Issue an event for the object.

    This is where they can also be accumulated, aggregated, grouped,
    and where the rate-limits should be maintained. It can (and should)
    be done by the client library, as it is done in the Go client.
    """

    # Prevent "event explosion", when core v1 events are handled and create other core v1 events.
    # This can happen with `EVERYTHING` without additional filters, or by explicitly serving them.
    if ref['apiVersion'] == 'v1' and ref['kind'] == 'Event':
        return

    # See #164. For cluster-scoped objects, use the current namespace from the current context.
    # It could be "default", but in some systems, we are limited to one specific namespace only.
    namespace_name: str = ref.get('namespace') or (await api.get_default_namespace()) or 'default'
    namespace = references.NamespaceName(namespace_name)
    full_ref: bodies.ObjectReference = copy.copy(ref)
    full_ref['namespace'] = namespace

    # Prevent a common case of event posting errors but shortening the message.
    if len(message) > MAX_MESSAGE_LENGTH:
        infix = CUT_MESSAGE_INFIX
        prefix = message[:MAX_MESSAGE_LENGTH // 2 - (len(infix) // 2)]
        suffix = message[-MAX_MESSAGE_LENGTH // 2 + (len(infix) - len(infix) // 2):]
        message = f'{prefix}{infix}{suffix}'

    now = datetime.datetime.now(datetime.timezone.utc)
    body = {
        'metadata': {
            'namespace': namespace,
            'generateName': settings.posting.event_name_prefix,
        },

        'action': 'Action?',
        'type': type,
        'reason': reason,
        'message': message,

        'reportingComponent': settings.posting.reporting_component,
        'reportingInstance': settings.posting.reporting_instance,
        'source': {'component': settings.posting.reporting_component},  # used in the "From" column in `kubectl describe`.

        'involvedObject': full_ref,

        'firstTimestamp': now.isoformat(),  # seen in `kubectl describe ...`
        'lastTimestamp': now.isoformat(),  # seen in `kubectl get events`
        'eventTime': now.isoformat(),
    }

    try:
        await api.post(
            url=resource.get_url(namespace=namespace),
            headers={'Content-Type': 'application/json'},
            payload=body,
            logger=logger,
            settings=settings,
        )

    # Events are helpful but auxiliary, they should not fail the handling cycle.
    # Yet we want to notice that something went wrong (in logs).
    except errors.APIError as e:
        logger.warning(f"Failed to post an event. Ignoring and continuing. "
                       f"Code: {e.code}. Message: {e.message}. Details: {e.details}"
                       f"Event: type={type!r}, reason={reason!r}, message={message!r}.")
    except aiohttp.ClientResponseError as e:
        logger.warning(f"Failed to post an event. Ignoring and continuing. "
                       f"Status: {e.status}. Message: {e.message}. "
                       f"Event: type={type!r}, reason={reason!r}, message={message!r}.")
    except aiohttp.ServerDisconnectedError as e:
        logger.warning(f"Failed to post an event. Ignoring and continuing. "
                       f"Message: {e.message}. "
                       f"Event: type={type!r}, reason={reason!r}, message={message!r}.")
    except aiohttp.ClientOSError:
        logger.warning(f"Failed to post an event. Ignoring and continuing. "
                       f"Event: type={type!r}, reason={reason!r}, message={message!r}.")
