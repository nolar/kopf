=========
Arguments
=========


.. kwarg:: kwargs

Forward compatibility kwargs
============================

``**kwargs`` is required in all handlers for the forward compatibility:
the framework can add new keywords in the future, and the existing handlers
should accept them without breaking, even if they do not use them.

It can be named ``**_`` to prevent the "unused variable" warnings by linters.


.. kwarg:: retry
.. kwarg:: started
.. kwarg:: runtime

Retrying and timing
===================

Most (but not all) of the handlers -- such as resource change detection,
resource daemons and timers, and activity handlers -- are capable of retrying
their execution in case of errors (see also: :doc:`errors`).
They provide kwargs regarding the retrying process:

``retry`` (``int``) is the sequential number of retry of this handler.
For the first attempt, it is ``0``, so it can be used in expressions
like ``if not retry: ...``.

``started`` (`datetime.datetime`) is the start time of the handler,
in case of retries & errors -- i.e. of the first attempt.

``runtime`` (`datetime.timedelta`) is the duration of the handler run,
in case of retries & errors -- i.e. since the first attempt.


.. kwarg:: settings

Operator configuration
======================

``settings`` is passed to activity handlers (but not to resource handlers).

It is an object with predefined nested structure of containers with values,
which defines the operator's behaviour. See also: `kopf.OperatorSettings`.

It can be modified if needed (usually in the startup handlers). Every operator
(if there are more than one in the same process) has its own config.

See also: :doc:`configuration`.


Resource-related kwargs
=======================

.. kwarg:: body
.. kwarg:: spec
.. kwarg:: meta
.. kwarg:: status
.. kwarg:: uid
.. kwarg:: name
.. kwarg:: namespace
.. kwarg:: labels
.. kwarg:: annotations

Body parts
----------

``body`` is the handled object's body, a read-only mapping (dict).

``spec``, ``meta``, ``status`` are aliases for relevant stanzas, and are
live-views into ``body['spec']``, ``body['metadata']``, ``body['status']``.

``namespace``, ``name``, ``uid`` can be used to identify the object being
handled, and are aliases for the respective fields in ``body['metadata']``.
If the values are not present for any reason (e.g. namespaced for cluster-scoped
objects), the fields are ``None`` -- unlike accessing the same fields by key,
when a ``KeyError`` is raised.

``labels`` and ``annotations`` are equivalents of ``body['metadata']['labels']``
and ``body['metadata']['annotations']`` if they exist. If not, these two behave
as empty dicts.


.. kwarg:: logger

Logging
-------


``logger`` is a per-object logger, with the messages prefixed with the object's
namespace/name.

Some of the log messages are also sent as Kubernetes events according to the
log level configuration (default is INFO, WARNINGs, ERRORs).


.. kwarg:: patch

Patching
--------

``patch`` is a mutable mapping (dict) with the object changes to be applied
after the handler. It is actively used internally by the framework itself,
and is shared to the handlers for convenience _(since patching happens anyway
in the framework, why make separate API calls for patching?)_.


.. kwarg:: memo

In-memory container
-------------------

``memo`` is an in-memory container for arbitrary runtime-only keys/fields
and values stored during the operator lifetime.
The values are shared by all the handlers for the same object.

The in-memory values are lost on operator restarts.
If the resource is deleted and re-created with the same name,
the in-memory values are also lost (technically, it is a new object).


Resource-watching kwargs
========================

For the resource watching handlers, an extra kwarg is provided:


.. kwarg:: event

API event
---------

``event`` is a raw JSON-decoded message received from Kubernetes API;
it is a dict with ``['type']`` & ``['object']`` keys.


Resource-changing kwargs
========================

Kopf provides functionality for change detection, and triggers the handlers
for those changes (not for every event coming from the Kubernetes API).
Few extra kwargs are provided for these handlers, exposing the detected changes:


.. kwarg:: reason

Causation
---------

``reason`` is a type of change detection (creation, update, deletion, resuming).
It is generally reflected in the handler decorator used, but can be useful for
the multi-purpose handlers pointing to the same function
(e.g. for ``@kopf.on.create`` + ``@kopf.on.resume`` pairs).


.. kwarg:: old
.. kwarg:: new
.. kwarg:: diff

Diffing
-------

``old`` & ``new`` are the old & new state of the object or a field within
the detected changes. The new state usually corresponds to :kwarg:`body`.

``diff`` is a list of changes of the object between old & new states.


Resource daemon kwargs
======================


.. kwarg:: stopped

Stop-flag
---------

The daemons also have ``stopped``. It is a flag object for sync daemons
to check if they should stop. See also: `DaemonStopperChecker`.

To check, ``.is_set()`` method can be called, or the object itself can be used
as a boolean expression: e.g. ``while not stopped: ...``.

Its ``.wait()`` method can be used to replace ``time.sleep()``
or ``asyncio.sleep()`` for faster (instant) termination on resource deletion.

See more: :doc:`daemons`.
