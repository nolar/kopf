import datetime

import kubernetes


def post_event(*, obj, type, reason, message=''):
    """
    Issue an event for the object.
    """

    now = datetime.datetime.utcnow()
    namespace = obj['metadata']['namespace']

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

    api = kubernetes.client.CoreV1Api()
    api.create_namespaced_event(
        namespace=namespace,
        body=body,
    )
