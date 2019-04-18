========
Concepts
========

**Kubernetes** is a container orchestrator.

It provides some basic primitives to orchestrate application deployments
on a low level ---such as the pods, jobs, deployments, services, ingresses,
persistent volumes and volume claims, secrets---
and allows a Kubernetes cluster to be extended with the arbitrary
custom resources and custom controllers.

On the top level, it consists of the Kubernetes API, through which the users
talk to Kubernetes, internal storage of the state of the objects (etcd),
and a collection of controllers. The command-line tooling (``kubectl``)
can also be considered as a part of the solution.

----

The **Kubernetes controller** is the logic (i.e. the behaviour) behind most
objects, both built-in and added as extension of Kubernetes.
Examples of objects are ReplicaSet and Pods, created when a Deployment object
is created, with the rolling version upgrades, and so on.

The main purpose of any controller is to bring the actual state
of the cluster to the desired state, as expressed with the resources/object
specifications.

----

The **Kubernetes operator** is one kind of the controllers, which orchestrates
objects of a specific kind, with some domain logic implemented inside.

The essential difference between operators and the controllers
is that operators are domain-specific controllers,
but not all controllers are necessary operators:
for example, the built-in controllers for pods, deployments, services, etc,
so as the extensions of the object's life-cycles based on the labels/annotations,
are not operators, but just controllers.

The essential similarity is that they both implement the same pattern:
watching the objects and reacting to the objects' events (usually the changes).

----

**Kopf** is a framework to build Kubernetes operators in Python.

As any framework, Kopf provides both the "outer" toolkit to run the operator,
to talk to the Kubernetes cluster, and to marshal the Kubernetes events
into the pure-Python functions of the Kopf-based operator,
and the "inner" libraries to assist with a limited set of common tasks
of manipulating the Kubernetes objects
(however, it is not a yet another Kubernetes client library).

.. seealso::

    See :doc:`/architecture`
    to understand how Kopf works in detail, and what it does exactly.

    See :doc:`/vision` and :doc:`/alternatives`
    to understand Kopf's self-positioning in the world of Kubernetes.

.. seealso::
    * https://en.wikipedia.org/wiki/Kubernetes
    * https://coreos.com/operators/
    * https://stackoverflow.com/a/47857073
    * https://github.com/kubeflow/tf-operator/issues/300
