import kopf


@kopf.on.event('kopfexamples')
def event_fn_with_error(**kwargs):
    raise Exception("Oops!")


@kopf.on.event('kopfexamples')
def normal_event_fn(event, **kwargs):
    print(f"Event received: {event!r}")


# Marks for the e2e tests (see tests/e2e/test_examples.py):
E2E_ALLOW_TRACEBACKS = True
E2E_SUCCESS_COUNTS = {'normal_event_fn': 2}
