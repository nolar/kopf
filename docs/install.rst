============
Installation
============

Prerequisites:

* Python >= 3.10 (CPython and PyPy are officially tested and supported).
* A Kubernetes cluster (k3d/k3s, minikube, OrbStack, Docker, AWS, GCP, etc).


Running without installation
============================

Using uvx
---------

To run Kopf as a tool via uvx_ with an operator:

.. code-block:: shell

    uvx kopf --help
    uvx kopf run --help
    uvx kopf run -v examples/01-minimal/example.py

.. _uvx: https://docs.astral.sh/uv/guides/tools/


Using docker
------------

A pre-built Docker image with all extras is available for quick
experimentation --- no local Python installation needed:

.. code-block:: bash

    # Minimize the credentials exposure.
    kubectl config view --minify --flatten > dev.kubeconfig

    # Run the operator locally, target a local cluster (host networking).
    docker run --rm -it --network=host \
        -v ./examples/01-minimal/example.py:/app/main.py:ro \
        -v ./dev.kubeconfig:/root/.kube/config:ro \
        ghcr.io/nolar/kopf

See :doc:`docker` for image variants, tags, and more usage examples.


Installing packages
===================

Using pip
---------

To install Kopf using ``pip`` (assuming you have a virtualenv activated)::

    pip install kopf


Using uv
--------

If you use uv_, once you are ready to build a project, add Kopf as a dependency:

.. code-block:: shell

    uv add kopf

.. _uv: https://docs.astral.sh/uv/


Cluster preparation
===================

Unless you use the standalone mode, create a few Kopf-specific custom resources
in the cluster. These are used to coordinate several instances of Kopf-based
operators so that they do not double-process the same resources — only one
operator will be active at a time:

.. code-block:: bash

    kubectl apply -f https://github.com/nolar/kopf/raw/main/peering.yaml

Depending on the security rules of your cluster, you might need :ref:`rbac`
resources applied, too (cluster- and user-specific; not covered here).


Example operators
=================

To run the example operators or the code snippets from the documentation,
apply this CRD for ``kopf.dev/v1/kopfexamples``. The examples will not operate
without it, but your own operator with your own resources does not need it:

.. code-block:: bash

    kubectl apply -f https://github.com/nolar/kopf/raw/main/examples/crd.yaml

You are ready to go. Run an operator using ``pip`` and ``virtualenv``:

.. code-block:: bash

    kopf --help
    kopf run --help
    kopf run examples/01-minimal/example.py

Using ``uv``:

.. code-block:: bash

    uv run kopf --help
    uv run kopf run --help
    uv run kopf run examples/01-minimal/example.py


Extras
======

To minimize the disk size impact of Kopf projects, some heavy dependencies
are omitted by default. You can add them as extras if you need or want them.

``full-auth``
-------------

If you use some of the managed Kubernetes services which require a sophisticated
authentication beyond the plain and simple username+password, fixed tokens,
or client SSL certs, add the ``full-auth`` extra with Kubernetes clients,
which in turn include those sophisticated authentication methods
(also see :ref:`authentication piggy-backing <auth-piggybacking>`):

.. code-block:: bash

    pip install 'kopf[full-auth]'
    uv add 'kopf[full-auth]'
    uvx --from 'kopf[full-auth]' kopf run -v examples/01-minimal/example.py

``uvloop``
----------

If you want extra I/O performance under the hood, install the uvloop_ extra
(also see :ref:`custom-event-loops`). It will be activated automatically
if installed, no extra flags or configuration is needed:

.. code-block:: bash

    pip install 'kopf[uvloop]'
    uv add 'kopf[uvloop]'
    uvx --from 'kopf[uvloop]' kopf run -v examples/01-minimal/example.py

.. _uvloop: https://github.com/MagicStack/uvloop

``dev``
-------

If you want mutating/validating webhooks (also see :doc:`admission`)
with self-signed certificates and/or ngrok tunneling,
install with the ``dev`` extra:

.. code-block:: bash

    pip install 'kopf[dev]'
    uv add 'kopf[dev]'
    uvx --from 'kopf[dev]' kopf run -v examples/01-minimal/example.py

.. warning::
    Self-signed certificates are unsafe for production environments.
    Ngrok tunnelling is not needed in production environments.
    This is supposed to be used in the development environments only.
    Hence the name.

.. note::
    This is the ``dev`` extra, not the ``dev`` dependency group.
    The dependency groups are available only when installing Kopf from source.
