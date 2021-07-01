================
Custom Resources
================

Custom Resource Definition
==========================

Let us define a CRD (custom resource definition) for our object:

.. code-block:: yaml
    :caption: crd.yaml
    :name: crd-yaml

    apiVersion: apiextensions.k8s.io/v1
    kind: CustomResourceDefinition
    metadata:
      name: ephemeralvolumeclaims.kopf.dev
    spec:
      scope: Namespaced
      group: kopf.dev
      names:
        kind: EphemeralVolumeClaim
        plural: ephemeralvolumeclaims
        singular: ephemeralvolumeclaim
        shortNames:
          - evcs
          - evc
      versions:
        - name: v1
          served: true
          storage: true
          schema:
            openAPIV3Schema:
              type: object
              properties:
                spec:
                  type: object
                  x-kubernetes-preserve-unknown-fields: true
                status:
                  type: object
                  x-kubernetes-preserve-unknown-fields: true

Note the short names: they can be used as the aliases on the command line,
when getting a list or an object of that kind.

And apply the definition to the cluster:

.. code-block:: bash

    kubectl apply -f crd.yaml

If you want to revert this operation (e.g., to try it again):

.. code-block:: bash

    kubectl delete crd ephemeralvolumeclaims.kopf.dev
    kubectl delete -f crd.yaml


Custom Resource Objects
=======================

Now, we can already create the objects of this kind, apply it to the cluster,
modify and delete them. Nothing will happen, since there is no implemented
logic behind the objects yet.

Let's make a sample object:

.. code-block:: yaml
    :caption: obj.yaml
    :name: obj-yaml

    apiVersion: kopf.dev/v1
    kind: EphemeralVolumeClaim
    metadata:
      name: my-claim

This is the minimal yaml file needed, with no spec or fields inside.
We will add them later.

Apply it to the cluster:

.. code-block:: bash

    kubectl apply -f obj.yaml

Get a list of the existing objects of this kind with one of the commands:

.. code-block:: bash

    kubectl get EphemeralVolumeClaim
    kubectl get ephemeralvolumeclaims
    kubectl get ephemeralvolumeclaim
    kubectl get evcs
    kubectl get evc

Please note that we can use the short names as specified
on the custom resource definition.

.. seealso::
    * kubectl imperative style (create/edit/patch/delete)
    * kubectl declarative style (apply)
