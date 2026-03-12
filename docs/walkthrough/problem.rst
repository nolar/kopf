==============
Sample Problem
==============

Throughout this user documentation, we solve
a small real-world problem with Kopf step by step,
presenting and explaining Kopf features one by one.


Problem Statement
=================

In Kubernetes, there are no ephemeral volumes of large sizes, e.g. 500 GB.
By ephemeral, we mean that the volume does not persist after it is used.
Such volumes can serve as a workspace for large data-crunching jobs.

There is `Local Ephemeral Storage`__, which allocates some space on a node's
root partition shared with the docker images and other containers,
but it is often limited in size depending on the node/cluster config:

__ https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#local-ephemeral-storage

.. code-block:: yaml

    kind: Pod
    spec:
      containers:
        - name: main
          resources:
            requests:
              ephemeral-storage: 1G
            limits:
              ephemeral-storage: 1G

There is a `PersistentVolumeClaim`__ resource kind, but it is persistent ---
meaning it is not deleted after use and can only be removed manually.

__ https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims

There is `StatefulSet`__, which has a volume claim template,
but the volume claim is again persistent,
and StatefulSets follow the same flow as Deployments, not Jobs.

__ https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/


Problem Solution
================

We will implement the ``EphemeralVolumeClaim`` object kind,
which will be directly equivalent to ``PersistentVolumeClaim``
(and will use it internally), but with a little extension:

It will be *designated* for one or more pods with specific selection criteria.

Once used, and all those pods are gone and are not going to be restarted,
the ephemeral volume claim will be deleted after a *grace period*.

For safety, there will be an *expiry period* for cases when the claim
was never used --- e.g. if the pod could not start for some reason ---
so that the claim does not remain stale forever.

The lifecycle of an ``EphemeralVolumeClaim`` is this:

* Created by a user with a template of ``PersistentVolumeClaim``
  and a designated pod selector (by labels).

* Waits until the claim is used at least once.

  * For at least N seconds to allow the pods to start safely.

  * For at most M seconds in case the pod failed to start
    but the claim was already created.

* Deletes the ``PersistentVolumeClaim`` after either the pod is finished,
  or the wait time has elapsed.

.. seealso::
    This documentation only highlights the main patterns & tricks of Kopf,
    but does not dive deep into the implementation of the operator's domain.
    The fully functional solution for ``EphemeralVolumeClaim`` resources,
    which is used for this documentation, is available at the following link:

    * https://github.com/nolar/ephemeral-volume-claims
