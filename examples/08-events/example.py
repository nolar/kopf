import kopf

# Marks for the e2e tests (see tests/e2e/test_examples.py):
E2E_TRACEBACKS = True


@kopf.on.event('zalando.org', 'v1', 'kopfexamples')
def event_fn_with_error(**kwargs):
    raise Exception("Oops!")


@kopf.on.event('zalando.org', 'v1', 'kopfexamples')
def normal_event_fn(event, **kwargs):
    print(f"Event received: {event!r}")
