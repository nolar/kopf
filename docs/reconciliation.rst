==============
Reconciliation
==============

Reconciliation is, in plain words, bringing the *actual state* of a system
to a *desired state* as expressed by the Kubernetes resources.
For example, starting as many pods as declared in a deployment,
especially when this declaration changes due to resource updates.

Kopf is not an operator, it is a framework to make operators.
Therefore, it knows nothing about the *desired state* or *actual state*
(or any *state* at all).

Kopf-based operators must implement the checks and reactions to the changes,
so that both states are synchronized according to the operator's concepts.

Kopf only provides a few ways and tools for achieving this easily.


Edge-based triggering
=====================

Normally, Kopf triggers the on-creation/on-update/on-deletion handlers
every time anything changes on the object, as reported by Kubernetes API.
It provides both the current state of the object and a diff list
with the last handled state. This is edge-based triggering (oversimplified).

This, however, fully excludes the *actual state* from the consideration,
thus breaking the whole idea of reconciliation of the *actual state*.

It might be good for some simplistic operators that do not have *actual state*
at all, and only care about the *desired state* as declared by the resources.

Yet, both high-level and low-level handlers are sufficient to keep track
of the *desired state*, and react when it changes, assuming they already
have the information on the *actual state*.

.. seealso::
    :doc:`handlers`

To keep track of the *actual state*, Kopf offers several ways:


Regularly scheduled timers
==========================

Timers are triggered on a regular schedule, regardless of whether anything
changes or does not change in the resource itself. You can use timers to
verify both the resource's body, and the state of other related resources
through API calls, update the original resource's status to keep track
of the *actual state*, and even bring the *actual state* to the *desired state*.

The little downside is that timers produce logs on every triggering,
which can be noisy, especially if triggered often. Also, such an operator
will not be very responsive to the changes in the *actual/desired states*
(only as responsive as the timer's interval defines it).

.. seealso::
    :doc:`timers`


Permanently running daemons
===========================

Daemons are long-running activities or background tasks dedicated
to each individual resource (object). They are typically an infinite cycle
of the same operation running until the resource is deleted (daemon is stopped).

The benefit of daemons over timers is that daemons do not log too much,
since they do not exit the function normally.

Besides, daemons can naturally use long-polling operations, which block
until something changes in the remote system, and react immediately
once it changes with no extra delays or polling intervals.

.. seealso::
    :doc:`daemons`


Level-based triggering
======================

In Kubernetes, level-based triggering is the core concept of reconciliation.
It implies that there is an *actual state* and a *desired state*.
The latter usually sits in ``spec``, while the former is calculated ---
it can come from inside the same Kubernetes cluster (children resources),
other clusters, or other non-Kubernetes systems.

As a generic pattern, Kopf recommends implementing such level-based triggering
and reconciliation the following way:

- Keep a timer or a daemon to regularly calculate the *actual state*,
  and store the result into the status stanza as one or several fields.

- For local Kubernetes resources as the *actual state*, use :doc:`indexing`
  instead of talking to the cluster API, in order to reduce the API load.

- Add on-field, or on-update/create handlers, or a low-level event handler
  for both the *actual state* and the *desired state* fields
  and react accordingly by bringing the actual state to the desired state.

An example for the in-cluster calculated *actual state* --- this is not
a full example (lacks wordy API calls for pods creation/termination),
but you can get the overall idea:

.. code-block:: python

    import kopf
    import random
    from typing import Any

    # Keep in-memory index of children resources, so that we avoid API calls doing the same.
    @kopf.index('pods', labels={'parent-kex': kopf.PRESENT})
    def kex_pods(body: kopf.Body, name: str, **_: Any) -> Any:
        parent_name = body.metadata.labels['parent-kex']
        return {parent_name: name}

    # Regularly calculate and save the *actual state* from an in-memory index.
    # If an in-memory index is absent, redesign this to make API calls to get the same data.
    @kopf.timer('kopfexamples', interval=10)
    def calculate_actual_state(name: str, kex_pods: kopf.Index, patch: kopf.Patch, **_: Any) -> None:
        actual_pods = kex_pods.get(name, [])
        patch.status['replicas'] = len(actual_pods)

    # React to changes in either the *desired* or *actual* states, and reconcile them.
    @kopf.on.event('kopfexamples')
    def react_on_state_changes(body: kopf.Body, name: str, **_: Any) -> None:
        actual_replicas = body.status.get('replicas', 0)
        desired_replicas = body.spec.get('replicas', 1)
        delta = desired_replicas - actual_replicas
        if delta > 0:
            print(f"Spawn {delta} new pods with labels: {{'parent-kex': {name!r}}}.")
        if delta < 0:
            running_pods = kex_pods.get(name, [])
            pods_to_terminate = random.sample(running_pods, k=min(-delta, len(running_pods))
            print(f"Terminate {-delta} random pods: {pods_to_terminate}")

Time-based polling works well for both in-cluster and external *actual states*,
and is in fact the only option for external *actual states* from third-party APIs.

For immediate reaction instead of polling, turn this timer into a daemon,
introduce a global operator-scoped condition (e.g., an :class:`asyncio.Condition`)
stored in :doc:`memos` on operator startup, await it in the daemon of the parent resource,
and notify it in the indexers of the children resources
(mind the synchronisation: the index changes slightly after the indexer exits).
