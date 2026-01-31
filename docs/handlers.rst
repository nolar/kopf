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

To hide the complexity of the state storing, Kopf provides a cause detection:
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

To register a handler for an event, use the ``@kopf.on`` decorator:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples')
    def my_handler(spec, **_):
        pass

All available decorators are described below.

Kopf only supports simple functions and static methods as handlers.
Class and instance methods are not supported.
For explanation and rationale, see the discussion in `#849`__ (briefly:
the semantics of handlers is vague when multiple instances exist or
multiple sub-classes inherit from the class, thus inheriting the handlers).

__ https://github.com/nolar/kopf/issues/849

Would you still want to use classes for namespacing, register the handlers
by using Kopf's decorators explicitly for specific instances/sub-classes thus
resolving the mentioned vagueness and giving the meaning to ``self``/``cls``:

.. code-block:: python

    import kopf

    class MyCls:
        def my_handler(self, spec, **kwargs):
            print(repr(self))

    instance = MyCls()
    kopf.on.create('kopfexamples')(instance.my_handler)


Event-watching handlers
=======================

Low-level events can be intercepted and handled silently, without
storing the handlers' status (errors, retries, successes) on the object.

This can be useful if the operator needs to watch over the objects
of another operator or controller, without adding its data.

The following event-handler is available:

.. code-block:: python

    import kopf

    @kopf.on.event('kopfexamples')
    def my_handler(event, **_):
        pass

If the event handler fails, the error is logged to the operator's log,
and then ignored.


.. note::
    Please note that the event handlers are invoked for *every* event received
    from the watching stream. This also includes the first-time listing when
    the operator starts or restarts.

    It is the developer's responsibility to make the handlers idempotent
    (re-executable with no duplicating side-effects).


State-changing handlers
=======================

Kopf goes further and beyond: it detects the actual causes of these events,
i.e. what happened to the object:

* Was the object just created?
* Was the object deleted (marked for deletion)?
* Was the object edited, and which fields specifically were edited,
  from which old values to which new values?

.. note::
    Worth noting that Kopf stores the status of the handlers, such as their
    progress or errors or retries, in the object itself (``.status`` stanza),
    which triggers its low-level events, but these events are not detected
    as separate causes, as there is nothing changed *essentially*.

The following 3 core cause-handlers are available:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples')
    def my_handler(spec, **_):
        pass

    @kopf.on.update('kopfexamples')
    def my_handler(spec, old, new, diff, **_):
        pass

    @kopf.on.delete('kopfexamples')
    def my_handler(spec, **_):
        pass

Despite the handlers see the full body of the resource object, they react
only to _essential_ changes, as implemented  by:class:`kopf.DiffBaseStorage`
or its descendants (:ref:`diffbase-storing`).

In particular, Kopf ignores the whole ``status`` stanza as non-essential,
and all fields of ``metadata`` except for ``labels`` & ``annotations`` —
it remains blind to changes in these fields unless explicitly told to see them.
For example, to react to changes in the status of ``kind: Job``:

.. code-block:: python

    import kopf

    @kopf.on.update('batch/v1', 'jobs', field='status')
    def job_status_changes(**_):
        pass

.. note::
    Kopf's finalizers will be added to the object when there are delete
    handlers specified. Finalizers block Kubernetes from fully deleting
    objects and Kubernetes will only actually delete objects when all
    finalizers are removed, i.e. only if the Kopf operator is running to
    remove them (check: :ref:`finalizers-blocking-deletion` for a workaround).
    If a delete handler is added but finalizers are not required to block the
    actual deletion, i.e. the handler is optional, the optional argument
    ``optional=True`` can be passed to the delete cause decorator.


Resuming handlers
=================

A special kind of handlers can be used for cases when the operator restarts
and detects an object that existed before:

.. code-block:: python

    import kopf

    @kopf.on.resume('kopfexamples')
    def my_handler(spec, **_):
        pass

This handler can be used to start threads or asyncio tasks or to update
a global state to keep it consistent with the actual state of the cluster.
With the resuming handler in addition to creation/update/deletion handlers,
no object will be left unattended even if it does not change over time.

The resuming handlers are guaranteed to execute only once per operator
lifetime for each resource object (except if errors are retried).

Normally, the resume handlers are mixed-in to the creation and updating
handling cycles, and are executed in the order they are declared.

It is a common pattern to declare both creation and resuming handler
pointing to the same function, so that this function is called either
when an object is created ("started) while the operator is alive ("exists"), or
when the operator is started ("created") when the object is existent ("alive"):

.. code-block:: python

    import kopf

    @kopf.on.resume('kopfexamples')
    @kopf.on.create('kopfexamples')
    def my_handler(spec, **_):
        pass

However, the resuming handlers are **not** called if the object has been deleted
during the operator downtime or restart, and the deletion handlers are now
being invoked.

This is done intentionally to prevent the cases when the resuming handlers start
threads/tasks or allocate the resources, and the deletion handlers stop/free
them: it can happen so that the resuming handlers would be executed after
the deletion handlers, thus starting threads/tasks and never stopping them.
For example:

.. code-block:: python

    import kopf

    TASKS = {}

    @kopf.on.delete('kopfexamples')
    async def my_handler(spec, name, **_):
        if name in TASKS:
            TASKS[name].cancel()

    @kopf.on.resume('kopfexamples')
    @kopf.on.create('kopfexamples')
    def my_handler(spec, **_):
        if name not in TASKS:
            TASKS[name] = asyncio.create_task(some_coroutine(spec))

In this example, if the operator starts and notices an object that is marked
for deletion, the deletion handler will be called, but the resuming handler
is not called at all, despite the object was noticed to exist out there.
Otherwise, there would be a resource (e.g. memory) leak.

If the resume handlers are still desired during the deletion handling, they
can be explicitly marked as compatible with the deleted state of the object
with ``deleted=True`` option:

.. code-block:: python

    import kopf

    @kopf.on.resume('kopfexamples', deleted=True)
    def my_handler(spec, **_):
        pass

In that case, both the deletion and resuming handlers will be invoked. It is
the developer's responsibility to ensure this does not lead to memory leaks.


Field handlers
==============

Specific fields can be handled instead of the whole object:

.. code-block:: python

    import kopf

    @kopf.on.field('kopfexamples', field='spec.somefield')
    def somefield_changed(old, new, **_):
        pass

There is no special detection of the causes for the fields,
such as create/update/delete, so the field-handler is efficient
only when the object is updated.


.. _subhandlers:

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

Sub-handlers can be implemented either imperatively
(where it requires :doc:`asynchronous handlers <async>` and ``async/await``):

.. code-block:: python

    import functools
    import kopf

    @kopf.on.create('kopfexamples')
    async def create_fn(spec, **_):
        fns = {}

        for item in spec.get('items', []):
            fns[item] = functools.partial(handle_item, item=item)

       await kopf.execute(fns=fns)

    def handle_item(item, *, spec, **_):
        pass

Or declaratively with decorators:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples')
    def create_fn(spec, **_):

        for item in spec.get('items', []):

            @kopf.subhandler(id=item)
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
    except as the read-only parsing of the inputs (e.g. :kwarg:`spec`),
    and generating the dynamic functions of the sub-handlers.
