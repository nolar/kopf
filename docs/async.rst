===========
Async/Await
===========

.. todo:: Fit this page into the walk-through sample story?

Kopf supports asynchronous handler functions:

.. code-block:: python

    import asyncio
    import kopf
    from typing import Any

    @kopf.on.create('kopfexamples')
    async def create_fn(spec: kopf.Spec, **_: Any) -> None:
        await asyncio.sleep(1.0)

Async functions have an additional benefit over non-async ones:
the full stack trace is available when exceptions occur
or IDE breakpoints are used, since async functions are executed
directly inside Kopf's event loop in the main thread.

Regular synchronous handlers, although supported for convenience,
are executed in parallel threads (via the default executor of the loop),
and can only show stack traces up to the thread entry point.

.. warning::
    As with any async coroutines, it is the developer's responsibility
    to ensure that all internal function calls are either
    ``await``\s of other async coroutines (e.g. ``await asyncio.sleep()``),
    or regular non-blocking function calls.

    Calling a synchronous function (e.g. HTTP API calls or ``time.sleep()``)
    inside an asynchronous function will block the entire operator process
    until the synchronous call finishes --- including other resources
    processed in parallel, and the Kubernetes event-watching/-queueing cycles.

    This can go unnoticed in a development environment
    with only a few resources and no external timeouts,
    but can cause serious problems in production environments under high load.
