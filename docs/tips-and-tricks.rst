=============
Tips & Tricks
=============


.. _never-again-filters:

Excluding handlers forever
==========================

Both successful executions and permanent errors of change-detecting handlers
only exclude these handlers from the current handling cycle, which is scoped
to the current change-set (i.e. one diff of an object).
On the next change, the handlers will be invoked again, regardless of their
previous permanent error.

The same is valid for the daemons: they will be spawned on the next operator
restart (assuming that one operator process is one handling cycle for daemons).

To prevent handlers or daemons from being invoked for a specific resource
ever again, even after the operator restarts, use annotations and filters
(or the same for labels or arbitrary fields with `when=` callback filtering):

.. code-block:: python

    import kopf

    @kopf.on.update('kopfexamples', annotations={'update-fn-never-again': kopf.ABSENT})
    def update_fn(patch, **_):
        patch.metadata.annotations['update-fn-never-again'] = 'yes'
        raise kopf.PermanentError("Never call update-fn again.")

    @kopf.daemon('kopfexamples', annotations={'monitor-never-again': kopf.ABSENT})
    async def monitor_kex(patch, **kwargs):
        patch.metadata.annotations['monitor-never-again'] = 'yes'

Such a never-again exclusion might be implemented as a feature of Kopf one day,
but it is not available now -- if not done explicitly as shown above.
