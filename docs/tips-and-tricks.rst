=============
Tips & Tricks
=============


.. _never-again-filters:

Excluding handlers forever
==========================

Both successful executions and permanent errors of change-detecting handlers
only exclude those handlers from the current handling cycle, which is scoped
to the current change set (i.e. one diff of an object).
On the next change, the handlers will be invoked again, regardless of any
previous permanent error.

The same applies to daemons: they will be spawned on the next operator
restart (assuming one operator process is one handling cycle for daemons).

To prevent handlers or daemons from being invoked for a specific resource
ever again, even after the operator restarts, use annotation filters
(or the equivalent for labels or arbitrary fields with ``when=`` callback filtering):

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.update('kopfexamples', annotations={'update-fn-never-again': kopf.ABSENT})
    def update_fn(patch: kopf.Patch, **_: Any) -> None:
        patch.metadata.annotations['update-fn-never-again'] = 'yes'
        raise kopf.PermanentError("Never call update-fn again.")

    @kopf.daemon('kopfexamples', annotations={'monitor-never-again': kopf.ABSENT})
    async def monitor_kex(patch: kopf.Patch, **_: Any) -> None:
        patch.metadata.annotations['monitor-never-again'] = 'yes'

Such a never-again exclusion may be implemented as a built-in Kopf feature one day,
but for now it is only available when implemented explicitly as shown above.
