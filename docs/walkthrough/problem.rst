==============
Sample Problem
==============

Throughout this user documentation, we try to solve
a little real-world problem with Kopf, step by step,
presenting and explaining all the Kopf features one by one.


Problem Statement
=================

In Kubernetes, there are no ephemeral volumes of big sizes, e.g. 500 GB.
By ephemeral, it means that the volume does not persist after it is used.
Such volumes can be used as workspace for large data-crunching jobs.

There is `Local Ephemeral Storage`__, which allocates some space on a node's
root partition, shared with the docker images and other containers,
but it is often limited in size depending on the node/cluster config:

__ https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#local-ephemeral-storage

.. code-block:: yaml

    kind: Pod
    spec:
      containers:
        - name: main
          resources:
            requests:
              ephemeral-storage: 10G
            limits:
              ephemeral-storage: 10G

There is a `PersistentVolumeClaim`__ resource kind, but it is persistent,
i.e. not deleted after they are created (only manually deletable).

__ https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims

There is `StatefulSet`__, which has the volume claim template,
but the volume claim is again persistent,
and the set does not follow the same flow as the Jobs do, more like the Deployments.

__ https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/


Problem Solution
================

We will implement the ``EphemeralVolumeClaim`` object kind,
which will be directly equivalent to ``PersistentVolumeClaim``
(and will use it internally), but with a little extension:

It will be *designated* for a pod or pods with a specific selection criteria.

Once used, and all those pods are gone and are not going to be restarted,
the ephemeral volume claim will be deleted after a *grace period*.

For safety, there will be an *expiry period* for the cases when the claim
was not used: e.g. if the pod could not start for some reasons,
so that the claim does not remain stale forever.

The lifecycle of an ``EphemeralVolumeClaim`` is this:

* Created by a user with a template of ``PersistentVolumeClaim``
  and a designated pod selector (by labels).

* Waits until the claim is used at least once.

  * At least for N seconds of the safe time to allow the pods to start.

  * At most for M seconds for the case when the pod has failed to start,
    but the claim was created.

* Deletes the ``PersistentVolumeClaim`` after either the pod is finished,
  or the wait time has elapsed.
