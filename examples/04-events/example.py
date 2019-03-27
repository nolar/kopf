"""
Send the custom events for the handled or other objects.
"""
import kopf


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(body, **kwargs):

    # The all-purpose function for the vent creation.
    kopf.event(body, type="SomeType", reason="SomeReason", message="Some message")

    # The shortcuts for the conventional events and common cases.
    kopf.info(body, reason="SomeReason", message="Some message")
    kopf.warn(body, reason="SomeReason", message="Some message")
    try:
        raise RuntimeError("Exception text.")
    except:
        kopf.exception(body, reason="SomeReason", message="Some exception:")
