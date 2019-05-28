import datetime

import kubernetes


def post_event(*, obj, type, reason, message=''):
    """
    Issue an event for the object.
    """

    now = datetime.datetime.utcnow()
    namespace = obj['metadata']['namespace']

    # Object reference - similar to the owner reference, but different.
    # TODO: reconstruct from `resource` once kind<->plural mapping is done. #57
    ref = dict(
        apiVersion=obj['apiVersion'],       # resource.version
        kind=obj['kind'],                   # resource.kind (~resource.plural)
        name=obj['metadata']['name'],
        uid=obj['metadata']['uid'],
        namespace=obj['metadata']['namespace'],
    )

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

        regarding=ref,
        # related=ref,

        event_time=now.isoformat() + 'Z',  # '2019-01-28T18:25:03.000000Z'
        deprecated_first_timestamp=now.isoformat() + 'Z',  # used in the "Age" column in `kubectl describe`.
    )

    api = kubernetes.client.EventsV1beta1Api()
    api.create_namespaced_event(
        namespace=namespace,
        body=body,
    )
