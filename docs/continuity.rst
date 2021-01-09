==========
Continuity
==========

Persistence
===========

Kopf does not have any database. It stores all the information directly
on the objects in the Kubernetes cluster (which means ``etcd`` usually).
All information is retrieved and stored via Kubernetes API.

Specifically:

* The cross-operator exchange is performed via peering objects of type
  ``KopfPeering`` or ``ClusterKopfPeering``
  (API versions: either ``kopf.dev/v1`` or ``zalando.org/v1``).
  See :doc:`peering` for more info.
* The last handled state of the object is stored in ``metadata.annotations``
  (the ``kopf.zalando.org/last-handled-configuration`` annotation).
  It is used to calculate diffs upon changes.
* The handlers' state (failures, successes, retries, delays) is stored
  in either ``metadata.annotations`` (``kopf.zalando.org/{id}`` keys),
  or in ``status.kopf.progress.{id}``, where ``{id}`` is the handler's id.

The persistent state locations can be configured to use different keys,
thus allowing multiple independent operators to handle the same resources
without overlapping with each other. The above-mentioned keys are the defaults.
See how to configure the stores in :doc:`configuration`
(at :ref:`progress-storing`, :ref:`diffbase-storing`).


Restarts
========

It is safe to kill the operator's pod (or process), and allow it to restart.

The handlers that succeeded previously will not be re-executed.
The handlers that did not execute yet, or were scheduled for retrying,
will be retried by a new operators pod/process from the point where
the old pod/process was terminated.

Restarting an operator will only affect the handlers currently being
executed in that operator at the moment of termination, as there is
no record that they have succeeded.


Downtime
========

If the operator is down and not running, any changes to the objects
are ignored and not handled. They will be handled when the operator starts:
every time a Kopf-based operator starts, it lists all objects of the served
resource kind, and checks for their state; if the state has changed since
the object was last handled (no matter how long time ago),
a new handling cycle starts.

Only the last state is taken into account. All the intermediate changes
are accumulated and handled together.
This corresponds to the Kubernetes's concept of eventual consistency
and level triggering (as opposed to edge triggering).

.. warning::
    If the operator is down, the objects may not be deleted,
    as they may contain the Kopf's finalizers in ``metadata.finalizers``,
    and Kubernetes blocks the deletion until all finalizers are removed.
    If the operator is not running, the finalizers will never be removed.
    See: :ref:`finalizers-blocking-deletion` for a work-around. 
