===========
Idempotence
===========

Kopf provides tools to make the handlers idempotent.

`kopf.register` function and `kopf.on.this` decorator allow to schedule
arbitrary sub-handlers for the execution in the current cycle.

`kopf.execute` coroutine executes arbitrary sub-handlers
directly in the place of invocation, and returns when all they have succeeded.

Every of the sub-handlers is tracked by Kopf, and will not be executed twice
within one handling cycle.

.. code-block:: python

    import functools
    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    async def create(spec, namespace, **kwargs):
        print("Entering create()!")  # executed ~7 times.
        await kopf.execute(fns={
            'a': create_a,
            'b': create_b,
        })
        print("Leaving create()!")  # executed 1 time only.

    async def create_a(retry, **kwargs):
        if retry < 2:
            raise kopf.HandlerRetryError("Not ready yet.", delay=10)

    async def create_b(retry, **kwargs):
        if retry < 6:
            raise kopf.HandlerRetryError("Not ready yet.", delay=10)

In this example, both ``create_a`` & ``create_b`` are submitted to Kopf
as the sub-handlers of ``create`` on every attempt to execute it.
It means, every ~10 seconds until both of the sub-handlers succeed,
and the main handler succeeds too.

The first one, ``create_a``, will succeed on the 3rd attempt after ~20s.
The second one, ``create_b``, will succeed only on the 7th attempt after ~60s.

However, despite ``create_a`` will be submitted every time when ``create``
and ``create_b`` are retried, it will not be executed in the 20s..60s range,
as it has succeeded already, and the record about this is stored on the object.

This approach can be used to perform operations, which needs the protection
from double-execution, such as the children object creation with randomly
generated names (e.g. Pods, Jobs, PersistentVolumeClaims, etc).

.. seealso::
    :doc:`persistence`, :ref:`Sub-handlers`.
