============
Installation
============

.. highlight:: bash

Prerequisites
=============

* Python >= 3.10 (CPython and PyPy are officially tested and supported).
* A Kubernetes cluster (k3d/k3s, minikube, OrbStack, Docker, AWS, GCP, etc).


Cluster resources
=================

Unless you use the standalone mode, create a few Kopf-specific custom resources
in the cluster. These are used to coordinate several instances of Kopf-based
operators so that they do not double-process the same resources — only one
operator will be active at a time::

    kubectl apply -f https://github.com/nolar/kopf/raw/main/peering.yaml

Optionally, if you are going to use the examples or the code snippets,
apply this CRD for ``kopf.dev/v1/kopfexamples``. The examples will not operate
without it, but your own operator with your own resources does not need it::

    kubectl apply -f https://github.com/nolar/kopf/raw/main/examples/crd.yaml

.. todo:: RBAC objects! kubectl apply -f rbac.yaml


Using pip
=========

To install Kopf using pip (assuming you have a virtualenv activated)::

    pip install kopf

You are ready to go::

    kopf --help
    kopf run --help
    kopf run -v examples/01-minimal/example.py


Using uv
========

If you use uv_, once you are ready to build a project, add Kopf as a dependency:

.. code-block:: shell

    uv add kopf

Alternatively, as a quick start, run Kopf as a tool via uvx_ with a sample
operator (assuming the cluster is up and running, all CRDs are applied):

.. code-block:: shell

    uvx kopf --help
    uvx kopf run --help
    uvx kopf run -v examples/01-minimal/example.py

.. _uv: https://docs.astral.sh/uv/
.. _uvx: https://docs.astral.sh/uv/guides/tools/


Extras
======

To minimize the disk size impact of Kopf projects, some heavy dependencies
are omitted by default. You can add them as extras if you need or want them.

If you use some of the managed Kubernetes services which require a sophisticated
authentication beyond the plain and simple username+password, fixed tokens,
or client SSL certs, add the ``full-auth`` extra with Kubernetes clients,
which in turn include those sophisticated authentication methods
(also see :ref:`authentication piggy-backing <auth-piggybacking>`)::

    pip install 'kopf[full-auth]'
    uv add 'kopf[full-auth]'
    uvx --from 'kopf[full-auth]' kopf run -v examples/01-minimal/example.py

If you want extra I/O performance under the hood, install uvloop_ extra
(also see :ref:`custom-event-loops`). It will be activated automatically
if installed, no extra flags or configuration is needed::

    pip install 'kopf[uvloop]'
    uv add 'kopf[uvloop]'
    uvx --from 'kopf[uvloop]' kopf run -v examples/01-minimal/example.py

.. _uvloop: https://github.com/MagicStack/uvloop
