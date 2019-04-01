============
Installation
============

.. highlight:: bash

To install Kopf::

    pip install kopf

Unless you use the standalone mode,
create few Kopf-specific custom resources in the cluster::

    kubectl apply -f peering.yaml

Optionally, if you are going to use the examples or the code snippets::

    kubectl apply -f examples/crd.yaml

.. todo:: RBAC objects! kubectl apply -f rbac.yaml

You are ready to go::

    kopf --help
    kopf run --help
    kopf run examples/01-minimal/example.py
