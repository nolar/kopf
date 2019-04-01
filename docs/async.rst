===========
Async/Await
===========

.. todo:: Fit this page into the walk-through sample story?

Kopf supports the asynchronous handler functions::

    import asyncio
    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    async def create_fn(spec, **_):
        await asyncio.sleep(1.0)

The async functions are a bit better than the regular functions,
since they are executed directly inside of the Kopf's own event loop
in the main thread, which makes the full stack trace available
for the exception stack traces and IDE breakpoints.

The regular synchronous handlers, despite supported for convenience,
are executed in the parallel threads (via the default executor of the loop),
and can only see the stack traces up to the thread entry functions.

.. warning::
    As with any async coroutines, it is the developer's responsibility
    to make sure that all the internal function calls are either
    ``await``\s of other async coroutines (e.g. ``await asyncio.sleep()``),
    or the regular non-blocking functions calls.

    Calling a synchronous function (e.g. HTTP API calls or ``time.sleep()``)
    inside of an asynchronous function will block the whole operator process
    until the synchronous call if finished, i.e. even other resources
    processed in parallel, and the Kubernetes event-watching/-queueing cycles.

    This can come unnoticed in the development environment
    with only few resources and no external timeouts,
    but can hit hard in the production environments with high load.
