==============
Error handling
==============

Kopf tracks the status of the handlers (except for the low-level event handlers)
catches the exceptions, and processes them from each of the handlers.

The last (or the final) exception is stored in the object's status,
and reported via the object's events.

.. note::
    Keep in mind, the Kubernetes events are often garbage-collected fast,
    e.g. less than 1 hour, so they are visible only soon after they are added.
    For persistence, the errors are also stored on the object's status.


Retriable errors
================

If an exception raised inherits from `kopf.HandlerRetryError`,
it will postpone the current handler for the next iteration,
which can be either immediately, or after some delay::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def create_fn(spec, **_):
        if not is_data_ready():
            raise kopf.HandlerRetryError("The data is not yet ready.", delay=60)

In that case, there is no need to sleep in the handler explicitly, thus blocking
any other events, causes, and generally any other handlers on the same object
from being handled (such as deletion or parallel handlers/sub-handlers).

.. note::
    The multiple handlers and the sub-handlers are implemented via this
    kind of errors: if there are handlers left after the current cycle,
    a special retriable error is raised, which marks the current cycle
    as to be retried immediately, where it continues with the remaining
    handlers.

    The only difference is that this special case produces less logs.


Fatal errors
============

If a raised exception inherits from `kopf.HandlerFatalError`, the handler
is considered as non-retriable and non-recoverable and completely failed.

Use this when the domain logic of the application means that there
is no need to retry over time, as it will not become better::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def create_fn(spec, **_):
        valid_until = datetime.datetime.fromisoformat(spec['validUntil'])
        if valid_until <= datetime.datetime.utcnow():
            raise kopf.HandlerFatalError("The object is not valid anymore.")



Regular errors
==============

Any other exceptions behave as either the retriable (default) or fatal,
depending on the settings.

.. todo::
    An example of an unexpected HTTP error?


Timeouts
========

The overall runtime of the handler can be limited::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', timeout=60*60)
    def create_fn(spec, **_):
        raise kopf.HandlerRetryError(delay=60)

If the handler is not succeeded withing this time, it is considered
as fatally failed.

If the handler is an async coroutine and it is still running at the moment,
an `asyncio.TimeoutError` is raised;
there is no equivalent way of terminating the synchronous functions by force.

By default, there is no timeout, so the retries continue forever.
