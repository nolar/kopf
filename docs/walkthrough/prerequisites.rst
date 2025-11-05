=================
Environment Setup
=================

We need a running Kubernetes cluster and some tools for our experiments.
If you have a cluster already preconfigured, you can skip this section.
Otherwise, let's install minikube locally (e.g. for MacOS):

* Python >= 3.10 (running in a venv is recommended, though is not necessary).
* `Install kubectl <https://kubernetes.io/docs/tasks/tools/install-kubectl/>`_
* :doc:`Install minikube </minikube>` (a local Kubernetes cluster)
* :doc:`Install Kopf </install>`

.. warning::
    Unfortunately, Minikube cannot handle the PVC/PV resizing,
    as it uses the HostPath provider internally.
    You can either skip the :doc:`updates` step of this tutorial
    (where the sizes of the volumes are changed),
    or you can use an external Kubernetes cluster
    with real dynamically sized volumes.
