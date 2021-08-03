============
Installation
============

.. highlight:: bash

To install Kopf::

    pip install kopf

If you use some of the managed Kubernetes services which require a sophisticated
authentication beyond username+password, fixed tokens, or client SSL certs
(also see :ref:`authentication piggy-backing <auth-piggybacking>`)::

    pip install kopf[full-auth]

Unless you use the standalone mode,
create few Kopf-specific custom resources in the cluster::

    kubectl apply -f https://github.com/nolar/kopf/raw/main/peering.yaml

Optionally, if you are going to use the examples or the code snippets::

    kubectl apply -f https://github.com/nolar/kopf/raw/main/examples/crd.yaml

.. todo:: RBAC objects! kubectl apply -f rbac.yaml

You are ready to go::

    kopf --help
    kopf run --help
    kopf run examples/01-minimal/example.py
