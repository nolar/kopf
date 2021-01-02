=======
Daemons
=======

Daemons are a special type of handler for background logic that accompanies
the Kubernetes resources during their life cycle.

Unlike event-driven short-running handlers declared with ``@kopf.on``,
daemons are started for every individual object when it is created
(or when an operator is started/restarted while the object exists),
and are capable of running indefinitely (or infinitely) long.

The object's daemons are stopped when the object is deleted
or the whole operator is exiting/restarting.


Spawning
========

To have a daemon accompanying a resource of some kind, decorate a function
with ``@kopf.daemon`` and make it run for long time or forever:

.. code-block:: python

    import asyncio
    import time
    import kopf

    @kopf.daemon('kopfexamples')
    async def monitor_kex_async(**kwargs):
        while True:
            ...  # check something
            await asyncio.sleep(10)

    @kopf.daemon('kopfexamples')
    def monitor_kex_sync(stopped, **kwargs):
        while not stopped:
            ...  # check something
            time.sleep(10)

Synchronous functions are executed in threads, asynchronous functions are
executed directly in the asyncio event loop of the operator -- same as with
regular handlers. See :doc:`async`.

The same executor is used both for regular sync handlers and for sync daemons.
If you expect large number of synchronous daemons (e.g. for large clusters),
make sure to pre-scale the executor accordingly.
See :doc:`configuration` (:ref:`configure-sync-handlers`).


Termination
===========

The daemons are terminated when either their resource is marked for deletion,
or the operator itself is exiting.

In both cases, the daemons are requested to terminate gracefully by setting
the :kwarg:`stopped` kwarg. The synchronous daemons MUST_, and asynchronous
daemons SHOULD_ check for the value of this flag as often as possible:

.. code-block:: python

    import asyncio
    import kopf

    @kopf.daemon('kopfexamples')
    def monitor_kex(stopped, **kwargs):
        while not stopped:
            time.sleep(1.0)
        print("We are done. Bye.")

The asynchronous daemons can skip these checks if they define the cancellation
timeout (see below). In that case, they can expect an `asyncio.CancelledError`
to be raised at any point of their code (specifically, at any ``await`` clause):

.. code-block:: python

    import asyncio
    import kopf

    @kopf.daemon('kopfexamples', cancellation_timeout=1.0)
    async def monitor_kex(**kwargs):
        try:
            while True:
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            print("We are done. Bye.")

With no cancellation timeout set, cancellation is not performed at all,
as it is unclear for how long should the coroutine be awaited. However,
it is cancelled when the operator exits and stops all "hung" left-over tasks
(not specifically daemons).

.. note::

    The MUST_ / SHOULD_ separation is due to Python having no way to terminate
    a thread unless the thread exits by its own. The :kwarg:`stopped` flag
    is a way to signal the thread it should exit. If :kwarg:`stopped` is not
    checked, the synchronous daemons will run forever or until an error happens.

.. _MUST: https://tools.ietf.org/rfc/rfc2119.txt
.. _SHOULD: https://tools.ietf.org/rfc/rfc2119.txt


Timeouts
========

The termination sequence parameters can be controlled when declaring a daemon:

.. code-block:: python

    import asyncio
    import kopf

    @kopf.daemon('kopfexamples',
                 cancellation_backoff=1.0, cancellation_timeout=3.0)
    async def monitor_kex(stopped, **kwargs):
        while not stopped:
            await asyncio.sleep(1)

There are three stages how the daemon is terminated:

* 1. Graceful termination:
  * ``stopped`` is set immediately (unconditionally).
  * ``cancellation_backoff`` is awaited (if set).
* 2. Forced termination -- only if ``cancellation_timeout`` is set:
  * `asyncio.CancelledError` is raised (for async daemons only).
  * ``cancellation_timeout`` is awaited (if set).
* 3a. Giving up and abandoning -- only if ``cancellation_timeout`` is set:
  * A `ResourceWarning` is issued for potential OS resource leaks.
  * The finalizer is removed, and the object is released for potential deletion.
* 3b. Forever polling -- only if ``cancellation_timeout`` is not set:
  * The daemon awaiting continues forever, logging from time to time.
  * The finalizer is not removed and the object remains blocked from deletion.

The ``cancellation_timeout`` is measured from the point when the daemon
is cancelled (forced termination begins), not from where the termination
itself begins; i.e., since the moment when the cancellation backoff is over.
The total termination time is ``cancellation_backoff + cancellation_timeout``.

.. warning::

    When the operator is exiting, it has its own timeout of 5 seconds
    for all "hung" tasks. This includes the daemons after they are requested
    to exit gracefully and all timeouts are reached.

    If the daemon termination takes longer than this for any reason,
    the daemon will be cancelled (by the operator, not by the daemon guard)
    regardless of the graceful timeout of the daemon. If this does not help,
    the operator will be waiting for all hung tasks until SIGKILL'ed.

.. warning::

    If the operator is running in a cluster, there can be timeouts set for a pod
    (``terminationGracePeriodSeconds``, the default is 30 seconds).

    If the daemon termination is longer than this timeout, the daemons will not
    be finished in full at the operator exit, as the pod will be SIGKILL'ed.

Kopf itself does not set any implicit timeouts for the daemons.
Either design the daemons to exit as fast as possible, or configure
``terminationGracePeriodSeconds`` and cancellation timeouts accordingly.


Safe sleep
==========

For synchronous daemons, it is recommended to use ``stopped.wait()``
instead of ``time.sleep()``: the wait will end when either the time is reached
(as with the sleep), or immediately when the stopped flag is set:

.. code-block:: python

    import kopf

    @kopf.daemon('kopfexamples')
    def monitor_kex(stopped, **kwargs):
        while not stopped:
            stopped.wait(10)

For asynchronous handlers, regular ``asyncio.sleep()`` should be sufficient,
as it is cancellable via `asyncio.CancelledError`. If cancellation is neither
configured nor desired, ``stopped.wait()`` can be used too (with ``await``):

.. code-block:: python

    import kopf

    @kopf.daemon('kopfexamples')
    async def monitor_kex(stopped, **kwargs):
        while not stopped:
            await stopped.wait(10)

This way, the daemon will exit as soon as possible when the :kwarg:`stopped`
is set, not when the next sleep is over. Therefore, the sleeps can be of any
duration while the daemon remains terminable (leads to no OS resource leakage).

.. note::

    Synchronous and asynchronous daemons get different types of stop-checker:
    with synchronous and asynchronous interfaces respectively.
    Therefore, they should be used accordingly: without or with ``await``.


Postponing
==========

Normally, daemons are spawned immediately once resource becomes visible
to the operator: i.e. on resource creation or operator startup.

It is possible to postpone the daemon spawning:

.. code-block:: python

    import asyncio
    import kopf

    @kopf.daemon('kopfexamples', initial_delay=30)
    async def monitor_kex(stopped, **kwargs):
        while True:
            await asyncio.sleep(1.0)


The start of the daemon will be delayed by 30 seconds after the resource
creation (or operator startup). For example, this can be used to give some time
for regular event-driven handlers to finish without producing too much activity.


Restarting
==========

It is generally expected that daemons are designed to run forever.
However, it is possible for a daemon to exit prematurely, i.e. before
the resource is deleted or the operator is exiting.

In that case, the daemon will not be restarted again during the lifecycle
of this resource in this operator process (however, it will be spawned again
if the operator restarts). This way, it becomes a long-running equivalent
of on-creation/on-resuming handlers.

To simulate restarting, raise `kopf.TemporaryError` with a delay set.

.. code-block:: python

    import asyncio
    import kopf

    @kopf.daemon('kopfexamples')
    async def monitor_kex(stopped, **kwargs):
        await asyncio.sleep(10.0)
        raise kopf.TemporaryError("Need to restart.", delay=10)

Same as with regular error handling, a delay of ``None`` means instant restart.

See also: :ref:`never-again-filters` to prevent daemons from spawning across
operator restarts.


Deletion prevention
===================

Normally, a finalizer is put on the resource if there are daemons running
for it -- to prevent its actual deletion until all the daemons are terminated.

Only after the daemons are terminated, the finalizer is removed to release
the object for actual deletion.

However, it is possible to have daemons that disobey the exiting signals
and continue running after the timeouts. In that case, the finalizer is
anyway removed, and the orphaned daemons are left to themselves.


Resource fields access
======================

The resource's current state is accessible at any time through regular kwargs
(see :doc:`kwargs`): :kwarg:`body`, :kwarg:`spec`, :kwarg:`meta`,
:kwarg:`status`, :kwarg:`uid`, :kwarg:`name`, :kwarg:`namespace`, etc.

The values are "live views" of the current state of the object as it is being
modified during its lifecycle (not frozen as in the event-driven handlers):

.. code-block:: python

    import random
    import time
    import kopf

    @kopf.daemon('kopfexamples')
    def monitor_kex(stopped, logger, body, spec, **kwargs):
        while not stopped:
            logger.info(f"FIELD={spec['field']}")
            time.sleep(1)

    @kopf.timer('kopfexamples', interval=2.5)
    def modify_kex_sometimes(patch, **kwargs):
        patch.spec['field'] = random.randint(0, 100)

Always access the fields through the provided kwargs, and do not store
them in local variables. Internally, Kopf substitutes the whole object's
body on every external change. Storing the field values to the variables
will remember their value as it was at that moment in time,
and will not be updated as the object changes.


Results delivery
================

As with any other handlers, it is possible for the daemons to return
arbitrary JSON-serializable values to be put on the resource's status:

.. code-block:: python

    import asyncio
    import kopf

    @kopf.daemon('kopfexamples')
    async def monitor_kex(stopped, **kwargs):
        await asyncio.sleep(10.0)
        return {'finished': True}


Error handling
==============

The error handling is the same as for all other handlers: see :doc:`errors`:

.. code-block:: python

    @kopf.daemon('kopfexamples',
                 errors=kopf.ErrorsMode.TEMPORARY, backoff=1, retries=10)
    def monitor_kex(retry, **_):
        if retry < 3:
            raise kopf.TemporaryError("I'll be back!", delay=1)
        elif retry < 5:
            raise EnvironmentError("Something happened!")
        else:
            raise kopf.PermanentError("Bye-bye!")

If a permanent error is raised, the daemon will never be restarted again.
Same as when the daemon exits on its own (but this could be reconsidered
in the future).


Filtering
=========

It is also possible to use the existing :doc:`filters`
to only spawn daemons for specific resources:

.. code-block:: python

    import time
    import kopf

    @kopf.daemon('kopfexamples',
                 annotations={'some-annotation': 'some-value'},
                 labels={'some-label': 'some-value'},
                 when=lambda name, **_: 'some' in name)
    def monitor_selected_kexes(stopped, **kwargs):
        while not stopped:
            time.sleep(1)

Other (non-matching) resources of that kind will be ignored.

The daemons will be executed only while the filtering criteria are met.
Both the resource's state and the criteria can be highly dynamic (e.g.
due to ``when=`` callable filters or labels/annotations value callbacks).

Once the daemon stops matching the criteria (either because the resource
or the criteria have been changed (e.g. for `when=` callbacks)),
the daemon is stopped. Once it matches the criteria again, it is re-spawned.

The checking is done only when the resource changes (any watch-event arrives).
The criteria themselves are not re-evaluated if nothing changes.

.. warning::

    A daemon that is being terminated is considered as still running, therefore
    it will not be re-spawned until the termination ends. It will be re-spawned
    the next time a watch-event arrives after the daemon has truly exited.


System resources
================

.. warning::

    A separate OS thread or asyncio task is started
    for each individual resource and each individual handler.

    Having hundreds or thousands of OS threads or asyncio tasks can consume
    system resources significantly. Make sure you only have daemons and timers
    with appropriate filters (e.g., by labels, annotations, or so).

    For the same reason, prefer to use async handlers (with properly designed
    async/await code), since asyncio tasks are a somewhat cheaper than threads.
    See :doc:`async` for details.
