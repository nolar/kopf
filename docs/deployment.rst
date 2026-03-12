==========
Deployment
==========

Kopf can be run outside the cluster, as long as the environment is
authenticated to access the Kubernetes API.
Normally, however, operators are deployed directly into the cluster.


Docker image
============

First, the operator must be packaged as a Docker image with Python 3.10 or newer:

.. code-block:: dockerfile
    :caption: Dockerfile
    :name: dockerfile

    FROM python:3.14
    RUN pip install kopf
    ADD . /src
    CMD kopf run /src/handlers.py --verbose

Build and push it to a repository of your choice.
Here, we use DockerHub_
(with the personal account "nolar" --- replace it with your own name or namespace;
you may also want to add version tags instead of the implied "latest"):

.. code-block:: bash

    docker build -t nolar/kopf-operator .
    docker push nolar/kopf-operator

.. _DockerHub: https://hub.docker.com/

.. seealso::
    Read the `DockerHub documentation <https://docs.docker.com/docker-hub/>`_
    for instructions on pushing and pulling Docker images.


Cluster deployment
==================

The best way to deploy the operator to the cluster is via the Deployment_
object: it will be kept alive automatically, and upgrades will be applied
properly on redeployment.

.. _Deployment: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/

For this, create the deployment file:

.. literalinclude:: deployment-depl.yaml
    :language: yaml
    :emphasize-lines: 6,18
    :caption: deployment.yaml
    :name: deployment-yaml

Note that there is only one replica. Keep it that way. If two or more operators
run in the cluster for the same objects, they will collide with each other
and the consequences are unpredictable.
During pod restarts, only one pod should be running at a time as well:
use ``.spec.strategy.type=Recreate`` (see the documentation_).

.. _documentation: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#recreate-deployment

Deploy it to the cluster:

.. code-block:: bash

    kubectl apply -f deployment.yaml

No services or ingresses are needed (unlike in typical web application examples),
since the operator does not listen for incoming connections
but only makes outgoing calls to the Kubernetes API.


.. _rbac:

RBAC
====

The pod where the operator runs must have permissions to access
and manipulate objects, both domain-specific and built-in ones.
For the example operator, those are:

* ``kind: ClusterKopfPeering`` for the cross-operator awareness (cluster-wide).
* ``kind: KopfPeering`` for the cross-operator awareness (namespace-wide).
* ``kind: KopfExample`` for the example operator objects.
* ``kind: Pod/Job/PersistentVolumeClaim`` as the children objects.
* And others as needed.

For this, RBAC__ (Role-Based Access Control) can be used
and attached to the operator's pod via a service account.

__: https://kubernetes.io/docs/reference/access-authn-authz/rbac/

Here is an example of what an RBAC config should look like
(remove the parts that are not needed: e.g. the cluster roles and bindings
for a strictly namespace-bound operator):

.. literalinclude:: deployment-rbac.yaml
    :caption: rbac.yaml
    :name: rbac-yaml
    :language: yaml

And the created service account is attached to the pods as follows:

.. literalinclude:: deployment-depl.yaml
    :language: yaml
    :lines: 1-2,5,12,16-20
    :emphasize-lines: 17
    :caption: deployment.yaml
    :name: deployment-service-account-yaml


Note that service accounts are always namespace-scoped.
There are no cluster-wide service accounts.
They must be created in the same namespace where the operator will run
(even if it is going to serve the whole cluster).
