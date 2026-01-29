=======
Startup
=======

The startup handlers are slightly different from the module-level code:
the actual tasks (e.g. API calls for resource watching) are not started
until all the startup handlers succeed.

The handlers run inside of the operator's event loop, so they can initialise
the loop-bound variables -- which is impossible in the module-level code:

.. code-block:: python

    import asyncio
    import kopf

    LOCK: asyncio.Lock

    @kopf.on.startup()
    async def startup_fn(logger, **kwargs):
        global LOCK
        LOCK = asyncio.Lock()  # uses the running asyncio loop by default

If any of the startup handlers fail, the operator fails to start
without making any external API calls.

.. note::

    If the operator is running in a Kubernetes cluster, there can be
    timeouts set for liveness/readiness checks of a pod.

    If the startup takes too long in total (e.g. due to retries),
    the pod can be killed by Kubernetes as not responding to the probes.

    Either design the startup activities to be as fast as possible,
    or configure the liveness/readiness probes accordingly.

    Kopf itself does not set any implicit timeouts for the startup activity,
    and it can continue forever (unless explicitly limited).
