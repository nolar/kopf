===========
Idempotence
===========

Kopf provides tools to make the handlers idempotent.

The :func:`kopf.register` function and the :func:`kopf.subhandler` decorator
allow scheduling arbitrary sub-handlers for execution in the current cycle.

:func:`kopf.execute` coroutine executes arbitrary sub-handlers
directly in the place of invocation, and returns when all of them have succeeded.

Every one of the sub-handlers is tracked by Kopf, and will not be executed
twice within one handling cycle.

.. code-block:: python

    import functools
    import kopf
    from typing import Any

    @kopf.on.create('kopfexamples')
    async def create(spec: kopf.Spec, namespace: str | None, **_: Any) -> None:
        print("Entering create()!")  # executed ~7 times.
        await kopf.execute(fns={
            'a': create_a,
            'b': create_b,
        })
        print("Leaving create()!")  # executed 1 time only.

    async def create_a(retry: int, **_: Any) -> None:
        if retry < 2:
            raise kopf.TemporaryError("Not ready yet.", delay=10)

    async def create_b(retry: int, **_: Any) -> None:
        if retry < 6:
            raise kopf.TemporaryError("Not ready yet.", delay=10)

In this example, both ``create_a`` & ``create_b`` are submitted to Kopf
as the sub-handlers of ``create`` on every attempt to execute it.
This repeats every ~10 seconds until both sub-handlers succeed
and the main handler succeeds too.

The first one, ``create_a``, will succeed on the 3rd attempt after ~20s.
The second one, ``create_b``, will succeed only on the 7th attempt after ~60s.

However, even though ``create_a`` will be submitted whenever ``create``
and ``create_b`` are retried, it will not be executed in the 20s..60s range,
as it has already succeeded, and the record of this is stored on the object.

This approach can be used to perform operations that need protection
from double-execution, such as the children object creation with randomly
generated names (e.g. Pods, Jobs, PersistentVolumeClaims, etc).

.. seealso::
    :ref:`persistence`, :ref:`subhandlers`.
