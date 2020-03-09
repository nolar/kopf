==============
Reconciliation
==============

Reconciliation is, in plain words, bringing the *actual state* of a system
to a *desired state* as expressed by the Kubernetes resources.
For example, starting as many pods, as it is declared in a deployment,
especially when this declaration changes due to resource updates.

Kopf is not an operator, it is a framework to make operators.
Therefore, it knows nothing about the *desired state* or *actual state*
(or any *state* at all).

Kopf-based operators must implement the checks and reactions to the changes,
so that both states are synchronised according to the operator's concepts.

Kopf only provides few ways and tools of achieving this easily.


Event-driven reactions
======================

Normally, Kopf triggers the on-creation/on-update/on-deletion handlers
every time anything changes on the object, as reported by Kubernetes API.
It provides both the current state of the object, and a diff with the last
handled state.

The event-driven approach is the best, as it saves system resources (RAM & CPU),
and does not trigger any activity when it is not needed, and does not consume
memory for keeping the object's last known state permanently in memory.

But it is more difficult to develop, and is not suitable for some cases:
e.g., when an external non-Kubernetes system is monitored via its own API.

.. seealso::
    :doc:`handlers`


Regularly scheduled timers
==========================

Timers are triggered on regular schedule, regardless of whether anything
changes or does not change in the resource itself. This can be used to
verify both the resource's body, and the state of other related resources
though API calls, and update the original resource's status/content.

.. seealso::
    :doc:`timers`


Permanently running daemons
===========================

As a last resort, a developer can implement their own background task,
which checks the status of the system and reacts when the "actual" state
diverts from the "desired" state.

.. seealso::
    :doc:`daemons`


What to use when?
=================

As a rule of thumb _(recommended, but not insisted)_, the following guidelines
can be used to decide which way of reconciliation to use in which cases:

* In the first place, try the event-driven approach by watching
  for the children resources (those belonging to the "actual" state).

  If there are many children resources for one parent resource,
  store their brief statuses on the parent's ``status.children.{id}``
  from every individual child, and react to the changes of ``status.children``
  in the parent resource.

* If the "desired" state can be queried with blocking waits
  (e.g. by running a ``GET`` query on a remote job/task/activity via an API,
  which blocks until the requested condition is reached),
  then use daemons to poll for the status, and process it as soon as it changes.

* If the "desired" state is not Kubernetes-related, maybe it is an external
  system accessed by an API, or if delays in reconciliation are acceptable,
  then use the timers.

* Only as the last resort, use the daemons with a ``while True`` cycle
  and explicit sleep.
