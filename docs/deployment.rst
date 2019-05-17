==========
Deployment
==========

Kopf can be executed out of the cluster, as long as the environment is
authenticated to access the Kubernetes API.
But normally, the operators are usually deployed directly to the clusters.


Docker image
============

First of all, the operator must be packaged as a docker image with Python 3.7:

.. code-block:: dockerfile
    :caption: Dockerfile
    :name: dockerfile

    FROM python:3.7
    ADD . /src
    RUN pip install kopf
    CMD kopf run /src/handlers.py --verbose

Build and push it to some repository of your choice.
Here, we will use DockerHub_
(with a personal account "nolar" -- replace it with your own name or namespace;
you may also want to add the versioning tags instead of the implied "latest"):

.. code-block:: bash

    docker build -t nolar/kopf-operator .
    docker push nolar/kopf-operator

.. _DockerHub: https://hub.docker.com/

.. seealso::
    Read `DockerHub documentation <https://docs.docker.com/docker-hub/>`_
    for how to use it to push & pull the docker images.


Cluster deployment
==================

The best way to deploy the operator to the cluster is via the Deployment_
object: in that case, it will be properly maintained alive and the versions
will be properly upgraded on the re-deployments.

.. _Deployment: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/

For this, create the deployment file:

.. literalinclude:: deployment-depl.yaml
    :language: yaml
    :emphasize-lines: 6,18
    :caption: deployment.yaml
    :name: deployment-yaml

Please note that there is only one replica. Keep it so. If there will be
two or more operators running in the cluster for the same objects,
they will collide with each other and the consequences are unpredictable.

Deploy it to the cluster:

.. code-block:: bash

    kubectl apply -f deployment.yaml

No services or ingresses are needed (unlike in the typical web-app examples),
as the operator is not listening for any incoming connections,
but only makes the outcoming calls to the Kubernetes API.


RBAC
====

The pod where the operator runs must have the permissions to access
and to manipulate the objects, both domain-specific and the built-in ones.
For the example operator, those are:

* ``kind: ClusterKopfPeering`` for the cross-operator awareness (cluster-wide).
* ``kind: KopfPeering`` for the cross-operator awareness (namespace-wide).
* ``kind: KopfExample`` for the example operator objects.
* ``kind: Pod/Job/PersistentVolumeClaim`` as the children objects.
* And others as needed.

For that, the RBAC_ (Role-Based Access Control) could be used
and attached to the operator's pod via a service account.

.. _RBAC: https://kubernetes.io/docs/reference/access-authn-authz/rbac/

Here is an example of what a RBAC config should look like
(remove the parts which are not needed: e.g. the cluster roles/bindings
for the strictly namespace-bound operator):

.. literalinclude:: deployment-rbac.yaml
    :caption: rbac.yaml
    :name: rbac-yaml
    :language: yaml

And the created service account is attached to the pods as follows:

.. literalinclude:: deployment-depl.yaml
    :language: yaml
    :lines: 1-2,5,10,14-17
    :emphasize-lines: 6
    :caption: deployment.yaml
    :name: deployment-service-account-yaml


Please note that the service accounts are always namespace-scoped.
There are no cluster-wide service accounts.
They must be created in the same namespace as the operator is going to run in
(even if it is going to serve the whole cluster).
