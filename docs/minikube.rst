========
Minikube
========

To develop the framework and operators in an isolated Kubernetes cluster,
use minikube_.

.. _minikube: https://github.com/kubernetes/minikube

macOS:

.. code-block:: bash

    brew install minikube
    brew install hyperkit

    minikube start --driver=hyperkit
    minikube config set driver hyperkit

Start the minikube cluster:

.. code-block:: bash

    minikube start
    minikube dashboard

It automatically creates and activates the kubectl context named ``minikube``.
If it does not, or if you have multiple clusters, activate it explicitly:

.. code-block:: bash

    kubectl config get-contexts
    kubectl config current-context
    kubectl config use-context minikube

To clean up minikube (and release CPU, RAM, and disk resources):

.. code-block:: bash

    minikube stop
    minikube delete

.. seealso::
    For even more information, read the `Minikube installation manual`__.

__ https://kubernetes.io/docs/tasks/tools/install-minikube/
