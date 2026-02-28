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

Kopf only provides a few ways and tools for achieving this easily.


Event-driven reactions
======================

Normally, Kopf triggers the on-creation/on-update/on-deletion handlers
every time anything changes on the object, as reported by Kubernetes API.
It provides both the current state of the object and a diff list
with the last handled state.

The event-driven approach is the best, as it saves system resources (RAM & CPU),
and does not trigger any activity when it is not needed and does not consume
memory for keeping the object's last known state permanently in memory.

But it is more difficult to develop, and is not suitable for some cases:
e.g., when an external non-Kubernetes system is monitored via its API.

.. seealso::
    :doc:`handlers`


Regularly scheduled timers
==========================

Timers are triggered on a regular schedule, regardless of whether anything
changes or does not change in the resource itself. This can be used to
verify both the resource's body, and the state of other related resources
through API calls, and update the original resource's status/content.

.. seealso::
    :doc:`timers`


Permanently running daemons
===========================

As a last resort, a developer can implement a background task,
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


Level-based triggering
======================

In Kubernetes, level-based triggering is the core concept of reconciliation.
It implies that there is an "actual state" and a "desired state".
The latter usually sits in ``spec``, while the former is calculated ---
it can come from inside the same Kubernetes cluster (children resources),
other clusters, or other non-Kubernetes systems.

As a generic pattern, Kopf recommends implementing such level-based triggering
and reconciliation the following way:

- Keep a timer or a daemon to regularly calculate the "actual state",
  and store the result into the status stanza as one or several fields.
- For local Kubernetes resources as the "actual state", use :doc:`indexing`
  instead of talking to the cluster API, in order to reduce the API load.
- Add on-field, or on-update/create handlers, or a low-level event handler
  for both the "actual state" and the "desired state" fields
  and react accordingly by bringing the actual state to the desired state.

An example for the in-cluster calculated actual state --- this is not
a full example (lacks wordy API calls for pods creation/termination),
but you can get the overall idea:

.. code-block:: python

    import random
    import kopf

    @kopf.index('pods', labels={'parent-kex': kopf.PRESENT})
    def kex_pods(body, name, **_):
        parent_name = body.metadata.labels['parent-kex']
        return {parent_name: name}

    @kopf.timer('kopfexamples', interval=10)
    def calculate_actual_state(name, kex_pods, patch, **_):
        actual_pods = kex_pods.get(name, [])
        patch.status['replicas'] = len(actual_pods)

    @kopf.on.event('kopfexamples')
    def react_on_state_changes(body, name, **_):
        actual_replicas = body.status.get('replicas', 0)
        desired_replicas = body.spec.get('replicas', 1)
        delta = desired_replicas - actual_replicas
        if delta > 0:
            print(f"Spawn {delta} new pods with labels: {{'parent-kex': {name!r}}}.")
        if delta < 0:
            running_pods = kex_pods.get(name, [])
            pods_to_terminate = random.sample(running_pods, k=min(-delta, len(running_pods))
            print(f"Terminate {-delta} random pods: {pods_to_terminate}")

Time-based polling is good both for in-cluster and external "actual states", and is in fact the only way for external "actual states" from third-party APIs.

For immediate reaction instead of timing, turn this timer into a daemon, introduce a global operator-scoped condition (e.g., an :class:`asyncio.Condition`) stored in :doc:`memos` on operator startup, await for it in the daemon of the parent resource, notify it in the indexers of the children resources (mind the synchronisation: the index changes slightly after the exit from the indexer).
