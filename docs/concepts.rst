========
Concepts
========

**Kubernetes** is a container orchestrator.
Over time, Kubernetes has become a standard de facto for the enterprise
infrastructure management, especially if it based on the microservices.

It provides some basic primitives to orchestrate the application deployments
on the low level ---such as the pods, jobs, deployments, services, ingresses,
persistent volumes and volume claims, secrets---
and allows the Kubernetes cluster to be extended with the arbitrary
custom resources and custom controllers.

On the top level, it consists of Kubernetes API, through which the users
talk to Kubernetes, internal storage of the state of the objects (etcd),
and a collection of the controllers. The command-line tooling (``kubectl``)
can also be considered as a part of the solution.

----

**Kubernetes controller** is the logic (i.e. the behaviour) behind most
of the objects, both built-in and added as the extension of Kubernetes.
For example, the ReplicaSet and Pod creation when a Deployment object
is created, the rolling version upgrades, and so on.

The main purpose of any controller is to bring the actual state
of the cluster to the desired state, as expressed with the resources/object
specifications.

----

**Kubernetes operator** is one kind of the controllers, which orchestrates
the objects of the specific kind, with some domain logic implemented in it.

The essential difference between the operators and the controllers
is that the operators are the domain-specific controllers,
but not all controllers are necessary the operators:
for example, the built-in controllers for pods, deployments, services, etc;
or the extensions of the object's life-cycles based on the labels/annotations.

The essential similarity is that they both implement the same pattern:
watching the objects and reacting to the objects' events (usually the changes).

----

**Kopf** is a framework to build the Kubernetes operators in Python.

As any framework, Kopf provides both the "outer" toolkit to run the operator,
to talk to Kubernetes cluster, and to marshall the Kubernetes events
into the pure-Python functions of the Kopf-based operator,
and the "inner" libraries to assist with a limited set of common tasks
of manipulating the Kubernetes objects
(however, it is not a yet another Kubernetes client library).

.. seealso::

    See :doc:`/architecture`
    to understand how does Kopf work in details and what exactly does it do.

    See :doc:`/vision` and :doc:`/alternatives`
    to understand Kopf's self-positioning in the world of Kubernetes.

.. seealso::
    * https://en.wikipedia.org/wiki/Kubernetes
    * https://coreos.com/operators/
    * https://stackoverflow.com/a/47857073
    * https://github.com/kubeflow/tf-operator/issues/300
