====================
Creating the objects
====================

Previously (:doc:`starting`),
we have created a skeleton operator and learned to start it and see the logs.
Now, let's add few meaningful reactions to solve our problem (:doc:`problem`).

We want to create a real ``PersistentVolumeClaim`` object
immediately when an ``EphemeralVolumeClaim`` is created this way:

.. code-block:: yaml
   :name: evc
   :caption: evc.yaml

    apiVersion: zalando.org/v1
    kind: EphemeralVolumeClaim
    metadata:
      name: my-claim
    spec:
      size: 10G

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
We will use the official Kubernetes client library:

.. code-block:: python
    :name: creation
    :linenos:
    :caption: ephemeral.py

    import kopf
    import kubernetes
    import yaml

    @kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
    def create_fn(meta, spec, namespace, logger, **kwargs):

        name = meta.get('name')
        size = spec.get('size')
        if not size:
            raise kopf.HandlerFatalError(f"Size must be set. Got {size!r}.")

        path = os.path.join(os.path.dirname(__file__), 'pvc.yaml')
        tmpl = open(path, 'rt').read()
        text = tmpl.format(name=name, size=size)
        data = yaml.load(text)

        api = kubernetes.client.CoreV1Api()
        obj = api.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=data,
        )

        logger.info(f"PVC child is created: %s", obj)

And let us try it in action (assuming the operator is running in the background):

.. code-block:: bash

    kubectl apply -f evc.yaml

Wait 1-2 seconds, and take a look:

.. code-block:: bash

    kubectl get pvc

Now, the PVC can be attached to the pods by the same name, as EVC is named.

.. seealso::
    See also :doc:`/handlers`, :doc:`/errors`, :doc:`/hierarchies`.
