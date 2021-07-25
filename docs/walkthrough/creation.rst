====================
Creating the objects
====================

Previously (:doc:`starting`),
we have created a skeleton operator and learned to start it and see the logs.
Now, let's add a few meaningful reactions to solve our problem (:doc:`problem`).

We want to create a real ``PersistentVolumeClaim`` object
immediately when an ``EphemeralVolumeClaim`` is created this way:

.. code-block:: yaml
    :name: evc
    :caption: evc.yaml

    apiVersion: kopf.dev/v1
    kind: EphemeralVolumeClaim
    metadata:
      name: my-claim
    spec:
      size: 1G

.. code-block:: bash

    kubectl apply -f evc.yaml

First, let's define a template of the persistent volume claim
(with the Python template string, so that no extra template engines are needed):

.. code-block:: yaml
    :name: pvc
    :caption: pvc.yaml

    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: "{name}"
      annotations:
        volume.beta.kubernetes.io/storage-class: standard
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: "{size}"


Let's extend our only handler.
We will use the official Kubernetes client library (``pip install kubernetes``):

.. code-block:: python
    :name: creation
    :caption: ephemeral.py

    import os
    import kopf
    import kubernetes
    import yaml

    @kopf.on.create('ephemeralvolumeclaims')
    def create_fn(spec, name, namespace, logger, **kwargs):

        size = spec.get('size')
        if not size:
            raise kopf.PermanentError(f"Size must be set. Got {size!r}.")

        path = os.path.join(os.path.dirname(__file__), 'pvc.yaml')
        tmpl = open(path, 'rt').read()
        text = tmpl.format(name=name, size=size)
        data = yaml.safe_load(text)

        api = kubernetes.client.CoreV1Api()
        obj = api.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=data,
        )

        logger.info(f"PVC child is created: {obj}")

And let us try it in action (assuming the operator is running in the background):

.. code-block:: bash

    kubectl apply -f evc.yaml

Wait 1-2 seconds, and take a look:

.. code-block:: bash

    kubectl get pvc

Now, the PVC can be attached to the pods by the same name, as EVC is named.

.. note::
    If you have to re-run the operator and hit an HTTP 409 error saying
    "persistentvolumeclaims "my-claim" already exists",
    then remove it manually:

    .. code-block:: bash

        kubectl delete pvc my-claim

.. seealso::
    See also :doc:`/handlers`, :doc:`/errors`, :doc:`/hierarchies`.
