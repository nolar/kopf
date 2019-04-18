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

All available decorators is described below.


Arguments
=========

The following keyword arguments are available to the handlers
(though some handlers may have some of them empty):

* ``body`` for the whole body of the handled objects.
* ``spec`` as an alias for ``body['spec']``.
* ``meta`` as an alias for ``body['metadata']``.
* ``status`` as an alias for ``body['status']``.
* ``retry`` (``int``) is the sequential number of retry of this handler.
* ``started`` (`datetime.datetime`) is the start time of the handler, in case of retries & errors.
* ``runtime`` (`datetime.timedelta`) is the duration of the handler run, in case of retries & errors.
* ``diff`` is a list of changes of the object (only for the update events).
* ``old`` is the old state of the object or a field (only for the update events).
* ``new`` is the new state of the object or a field (only for the update events).
* ``logger`` is a per-object logger, with the messages prefixed with the object's namespace/name.
* ``event`` is the raw event as received from the Kubernetes API.
* ``cause`` is the processed cause of the handler as detected by the framework (create/update/delete).
* ``patch`` is a dict with the object changes to applied after the handler.

``**kwargs`` (or ``**_`` to stop the linting warnings on the unused variables)
is required for the forward compatibility: the framework can add new keywords
in the future, and the existing handlers should accept them and not break.


Cause handlers
==============

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
