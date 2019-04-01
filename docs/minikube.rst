========
Minikube
========

.. highlight:: bash

To develop the framework and the operators in an isolated Kubernetes cluster,
use minikube_.

.. _minikube: https://github.com/kubernetes/minikube

MacOS::

    brew install docker-machine-driver-hyperkit
    sudo chown root:wheel /usr/local/opt/docker-machine-driver-hyperkit/bin/docker-machine-driver-hyperkit
    sudo chmod u+s /usr/local/opt/docker-machine-driver-hyperkit/bin/docker-machine-driver-hyperkit

    brew cask install minikube
    minikube config set vm-driver hyperkit

Start the minikube cluster::

    minikube start
    minikube dashboard

It automatically creates and activates the kubectl context named ``minikube``.
If not, or if you have multiple clusters, activate it explicitly::

    kubectl config get-contexts
    kubectl config current-context
    kubectl config use-context minikube

For the minikube cleanup (to release the CPU/RAM/disk resources)::

    minikube stop
    minikube delete

.. seealso::
    For even more information, read the `Minikube installation manual`__.

__ https://kubernetes.io/docs/tasks/tools/install-minikube/
