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


Temporary errors
================

If an exception raised inherits from :class:`kopf.TemporaryError`,
it will postpone the current handler for the next iteration,
which can happen either immediately, or after some delay:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples')
    def create_fn(spec, **_):
        if not is_data_ready():
            raise kopf.TemporaryError("The data is not yet ready.", delay=60)

In that case, there is no need to sleep in the handler explicitly, thus blocking
any other events, causes, and generally any other handlers on the same object
from being handled (such as deletion or parallel handlers/sub-handlers).

.. note::
    The multiple handlers and the sub-handlers are implemented via this
    kind of errors: if there are handlers left after the current cycle,
    a special retriable error is raised, which marks the current cycle
    as to be retried immediately, where it continues with the remaining
    handlers.

    The only difference is that this special case produces fewer logs.


Permanent errors
================

If a raised exception inherits from :class:`kopf.PermanentError`, the handler
is considered as non-retriable and non-recoverable and completely failed.

Use this when the domain logic of the application means that there
is no need to retry over time, as it will not become better:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples')
    def create_fn(spec, **_):
        valid_until = datetime.datetime.fromisoformat(spec['validUntil'])
        if valid_until <= datetime.datetime.now(datetime.timezone.utc):
            raise kopf.PermanentError("The object is not valid anymore.")

See also: :ref:`never-again-filters` to prevent handlers from being invoked
for the future change-sets even after the operator restarts.


Regular errors
==============

Kopf assumes that any arbitrary errors
(i.e. not :class:`kopf.TemporaryError` and not :class:`kopf.PermanentError`)
are the environment's issues and can self-resolve after some time.

As such, as default behaviour,
Kopf retries the handlers with arbitrary errors
infinitely until the handlers either succeed or fail permanently.

The reaction to the arbitrary errors can be configured:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples', errors=kopf.ErrorsMode.PERMANENT)
    def create_fn(spec, **_):
        raise Exception()

Possible values of ``errors`` are:

* ``kopf.ErrorsMode.TEMPORARY`` (the default).
* ``kopf.ErrorsMode.PERMANENT`` (prevent retries).
* ``kopf.ErrorsMode.IGNORED`` (same as in the resource watching handlers).


Timeouts
========

The overall runtime of the handler can be limited:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples', timeout=60*60)
    def create_fn(spec, **_):
        raise kopf.TemporaryError(delay=60)

If the handler is not succeeded within this time, it is considered
as fatally failed.

If the handler is an async coroutine and it is still running at the moment,
an :class:`asyncio.TimeoutError` is raised;
there is no equivalent way of terminating the synchronous functions by force.

By default, there is no timeout, so the retries continue forever.


Retries
=======

The number of retries can be limited too:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples', retries=3)
    def create_fn(spec, **_):
        raise Exception()

Once the number of retries is reached, the handler fails permanently.

By default, there is no limit, so the retries continue forever.


Backoff
=======

The interval between retries on arbitrary errors, when an external environment
is supposed to recover and be able to succeed the handler execution,
can be configured:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples', backoff=30)
    def create_fn(spec, **_):
        raise Exception()

The default is 60 seconds.

.. note::

    This only affects the arbitrary errors. When :class:`kopf.TemporaryError`
    is explicitly used, the delay should be configured with ``delay=...``.
