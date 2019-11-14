========
Handlers
========

.. todo:: Multiple handlers per script.

Handlers are Python functions with the actual behaviour
of the custom resources.

They are called when any custom resource (within the scope of the operator)
is created, modified, or deleted.

Any operator built with Kopf is based on handlers.


Events & Causes
===============

Kubernetes only notifies when something is changed in the object,
but it does not clarify what was changed.

More on that, since Kopf stores the state of the handlers on the object itself,
these state changes also cause the events, which are seen by the operators
and any other watchers.

To hide the complexity of the state storing, Kopf provides the cause detection:
whenever an event happens for the object, the framework detects what happened
actually, as follows:

* Was the object just created?
* Was the object deleted (marked for deletion)?
* Was the object edited, and which fields specifically were edited,
  from what old values into what new values?

These causes, in turn, trigger the appropriate handlers, passing the detected
information to the keyword arguments.


Registering
===========

To register a handler for an event, use the `kopf.on` decorator::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def my_handler(spec, **_):
        pass

All available decorators are described below.


Arguments
=========

The following keyword arguments are available to the handlers
(though some handlers may have some of them empty):

* ``memo`` for arbitrary in-memory runtime-only keys/fields and values stored
  during the operator lifetime, per-object; they are lost on operator restarts.
* ``body`` for the whole body of the handled objects.
* ``spec`` as an alias for ``body['spec']``.
* ``meta`` as an alias for ``body['metadata']``.
* ``status`` as an alias for ``body['status']``.
* ``namespace``, ``name``, ``uid`` to identify the object being handled,
  and are aliases for the respective fields in ``body['metadata']``.
* ``logger`` is a per-object logger, with the messages prefixed with the object's namespace/name.
* ``patch`` is a dict with the object changes to be applied after the handler.

High-level cause-handlers have additional keyword arguments reflecting their status:

* ``cause`` is the processed cause of the handler as detected by the framework (create/update/delete).
* ``retry`` (``int``) is the sequential number of retry of this handler.
* ``started`` (`datetime.datetime`) is the start time of the handler, in case of retries & errors.
* ``runtime`` (`datetime.timedelta`) is the duration of the handler run, in case of retries & errors.
* ``diff`` is a list of changes of the object (only for the update events).
* ``old`` is the old state of the object or a field (only for the update events).
* ``new`` is the new state of the object or a field (only for the update events).

Low-level event-handlers have a slightly different set of keyword arguments:

** ``event`` for the raw event received; it is a dict with ``['type']`` & ``['object']`` keys.

``**kwargs`` (or ``**_`` to stop the linting warnings on the unused variables)
is required for the forward compatibility: the framework can add new keywords
in the future, and the existing handlers should accept them and not break.


Event-watching handlers
=======================

Low-level events can be intercepted and handled silently, without
storing the handlers' status (errors, retries, successes) on the object.

This can be useful if the operator needs to watch over the objects
of another operator or controller, without adding its own data.

The following event-handler is available::

    import kopf

    @kopf.on.event('zalando.org', 'v1', 'kopfexamples')
    def my_handler(event, **_):
        pass

If the event handler fails, the error is logged to the operator's log,
and then ignored.


.. note::
    Please note that the event handlers are invoked for *every* event received
    from the watching stream. This also includes the first-time listing when
    the operator starts or restarts.

    It is the developer's responsibility to make the handlers idempotent
    (re-executable with do duplicating side-effects).


State-changing handlers
=======================

Kopf goes further and beyond: it detects the actual causes of these events,
i.e. what actually happened to the object:

* Was the object just created?
* Was the object deleted (marked for deletion)?
* Was the object edited, and which fields specifically were edited,
  from which old values to which new values?

.. note::
    Worth noting that Kopf stores the status of the handlers, such as their
    progress or errors or retries, in the object itself (in its ``status``),
    which triggers its own low-level events, but these events are not detected
    as separate causes, as there is nothing changed *essentially*.

The following 3 core cause-handlers are available::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def my_handler(spec, **_):
        pass

    @kopf.on.update('zalando.org', 'v1', 'kopfexamples')
    def my_handler(spec, old, new, diff, **_):
        pass

    @kopf.on.delete('zalando.org', 'v1', 'kopfexamples')
    def my_handler(spec, **_):
        pass

.. note::
    Kopf's finalizers will be added to the object when there are delete
    handlers specified. Finalizers block Kubernetes from fully deleting
    objects, and Kubernetes will only actually delete objects when all
    finalizers are removed, i.e. only if the Kopf operator is running to
    remove them (check: :ref:`finalizers-blocking-deletion` for a work-around).
    If a delete handler is added but finalizers are not required to block the
    actual deletion, i.e. the handler is optional, the optional argument
    ``optional=True`` can be passed to the delete cause decorator.


Resuming handlers
=================

An special kind of handlers can be used for cases when the operator restarts
and detects an object that existed before::

    @kopf.on.resume('zalando.org', 'v1', 'kopfexamples')
    def my_handler(spec, **_):
        pass

This handler can be used to start threads or asyncio tasks or to update
a global state to keep it consistent with the actual state of the cluster.
With the resuming handler in addition to creation/update/deletion handlers,
no object will be left unattended even if it does not change over time.

The resuming handlers are guaranteed to execute only once per operator
life time for each individual resource (except if errors are retried).

Normally, the resume handlers are mixed-in to the creation and updating
handling cycles, and are executed in the order they are declared.

It is a common pattern to declare both creation and resuming handler
pointing to the same function, so that this function is called either
when an object is created ("started) while the operator is alive ("exists"), or
when the operator is started ("created") when the object is existent ("alive")::

    @kopf.on.resume('zalando.org', 'v1', 'kopfexamples')
    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def my_handler(spec, **_):
        pass

However, the resuming handlers are **not** called if the object has been deleted
during the operator downtime or restart, and the deletion handlers are now
being invoked.

This is done intentionally to prevent the cases when the resuming handlers start
threads/tasks or allocate the resources, and the deletion handlers stop/free
them: it can happen so that the resuming handlers would be executed after
the deletion handlers, thus starting threads/tasks and never stopping them.
For example::

    TASKS = {}

    @kopf.on.delete('zalando.org', 'v1', 'kopfexamples')
    async def my_handler(spec, name, **_):
        if name in TASKS:
            TASKS[name].cancel()

    @kopf.on.resume('zalando.org', 'v1', 'kopfexamples')
    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def my_handler(spec, **_):
        if name not in TASKS:
            TASKS[name] = asyncio.create_task(some_coroutine(spec))

In this example, if the operator starts and notices an object that is marked
for deletion, the deletion handler will be called, but the resuming handler
is not called at all, despite the object was noticed to exist out there.
Otherwise, there would be a resource (e.g. memory) leak.

If the resume handlers are still desired during the deletion handling, they
can be explicitly marked as compatible with the deleted state of the object
with ``deleted=True`` option::

    @kopf.on.resume('zalando.org', 'v1', 'kopfexamples', deleted=True)
    def my_handler(spec, **_):
        pass

In that case, both the deletion and resuming handlers will be invoked. It is
the developer's responsibility to ensure this does not lead to the memory leaks.


Field handlers
==============

Specific fields can be handled instead of the whole object::

    import kopf

    @kopf.on.field('zalando.org', 'v1', 'kopfexamples', field='spec.somefield')
    def somefield_changed(old, new, **_):
        pass

There is no special detection of the causes for the fields,
such as create/update/delete, so the field-handler is efficient
only when the object is updated.


Sub-handlers
============

.. warning::
    Sub-handlers are an advanced topic. Please, make sure you understand
    the regular handlers first, so as the handling cycle of the framework.

A common example for this feature are the lists defined in the spec,
each of which should be handled with a handler-like approach
rather than explicitly -- i.e. with the error tracking, retries, logging,
progress and status reporting, etc.

This can be used with dynamically created functions, such as lambdas,
partials (`functools.partial`), or the inner functions in the closures:

.. code-block:: yaml

    spec:
      items:
        - item1
        - item2

Sub-handlers can be implemented either imperatively::

    import functools
    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def create_fn(spec, **_):
        fns = {}

        for item in spec.get('items', []):
            fns[item] = functools.partial(handle_item, item=item)

       kopf.execute(fns)

    def handle_item(item, *, spec, **_):
        pass

Or decoratively::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def create_fn(spec, **_):

        for item in spec.get('items', []):

            @kopf.on.this(id=item)
            def handle_item(item=item, **_):

                pass

Both of these ways are equivalent.
It is a matter of taste and preference which one to use.

The sub-handlers will be processed by all the standard rules and cycles
of the Kopf's handling cycle, as if they were the regular handlers
with the ids like ``create_fn/item1``, ``create_fn/item2``, etc.

.. warning::
    The sub-handler functions, their code or their arguments,
    are not remembered on the object between the handling cycles.

    Instead, their parent handler is considered as not finished,
    and it is called again and again to register the sub-handlers
    until all the sub-handlers of that parent handler are finished,
    so that the parent handler also becomes finished.

    As such, the parent handler SHOULD NOT produce any side-effects
    except as the read-only parsing of the inputs (e.g. ``spec``),
    and generating the dynamic functions of the sub-handlers.


Filtering
=========

It is possible to only execute handlers when the object that triggers a handler
matches certain filters.

The following filters are available for all event, cause, and field handlers:

* Match an object's label and value::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': 'somevalue'})
    def my_handler(spec, **_):
        pass

* Match on the existence of an object's label::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': None})
    def my_handler(spec, **_):
        pass

* Match an object's annotation and value::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': 'somevalue'})
    def my_handler(spec, **_):
        pass

* Match on the existence of an object's annotation::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': None})
    def my_handler(spec, **_):
        pass


Startup handlers
================

The startup handlers are slightly different from the module-level code:
the actual tasks (e.g. API calls for resource watching) are not started
until all the startup handlers succeed.

The handlers run inside of the operator's event loop, so they can initialise
the loop-bound variables -- which is impossible in the module-level code::

    import asyncio
    import kopf

    LOCK: asyncio.Lock

    @kopf.on.startup()
    async def startup_fn(logger, **kwargs):
        global LOCK
        LOCK = asyncio.Lock()  # uses the running asyncio loop by default

If any of the startup handlers fails, the operator fails to start
without making any external API calls.

.. note::

    If the operator is running in a Kubernetes cluster, there can be
    timeouts set for liveness/readiness checls of a pod.

    If the startup takes too longer in total (e.g. due to retries),
    the pod can be killed by Kubernetes as not responding to the probes.

    Either design the startup activities to be as fast as possible,
    or configure the liveness/readiness probes accordingly.

    Kopf itself does not set any implicit timeouts for the startup activity,
    and it can continue forever (unless explicitly limited).


Cleanup handlers
================

The cleanup handlers are executed when the operator exits
either by a signal (e.g. SIGTERM), or by catching an exception,
or by raising the stop-flag, or by cancelling the operator's task
(for :doc:`embedded operators </embedding>`)::

    import kopf

    @kopf.on.cleanup()
    async def cleanup_fn(logger, **kwargs):
        pass

The cleanup handlers are not guaranteed to be fully executed if they take
too long -- due to a limited graceful period or non-graceful termination.

Similarly, the cleanup handlers are not executed if the operator
is force-killed with no possibility to react (e.g. by SIGKILL).

.. note::

    If the operator is running in a Kubernetes cluster, there can be
    timeouts set for graceful termination of a pod
    (``terminationGracePeriodSeconds``, the default is 30 seconds).

    If the cleanup takes longer than that in total (e.g. due to retries),
    the activity will not be finished in full,
    as the pod will be SIGKILL'ed by Kubernetes.

    Either design the cleanup activities to be as fast as possible,
    or configure ``terminationGracePeriodSeconds`` accordingly.

    Kopf itself does not set any implicit timeouts for the cleanup activity,
    and it can continue forever (unless explicitly limited).
