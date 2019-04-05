====================
Updating the objects
====================

Previously (:doc:`creation`),
we have implemented a handler for the creation of an ``EphemeralVolumeClaim`` (EVC),
and created the corresponding ``PersistantVolumeClaim`` (PVC).

What will happen if we change the size of the EVC when it already exists?
E.g., with:

.. code-block:: bash

    kubectl edit evc my-claim

Or by patching it:

.. code-block:: bash

    kubectl patch evc my-claim -p '{"spec": {"resources": {"requests": {"storage": "100G"}}}}'

The PVC must be updated accordingly to match its parent EVC.

First, we have to remember the name of the created PVC:
Let's extend the creation handler we already have from the previous step
with one additional line:

.. code-block:: python
    :linenos:
    :caption: ephemeral.py
    :emphasize-lines: 23

    @kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
    def create_fn(spec, meta, namespace, logger, **kwargs):

        name = meta.get('name')
        size = spec.get('size')
        if not size:
            raise kopf.HandlerFatalError(f"Size must be set. Got {size!r}.")

        path = os.path.join(os.path.dirname(__file__), 'pvc-tpl.yaml')
        tmpl = open(path, 'rt').read()
        text = tmpl.format(size=size, name=name)
        data = yaml.load(text)

        kopf.adopt(data, owner=body)

        api = kubernetes.client.CoreV1Api()
        obj = api.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=data,
        )

        logger.info(f"PVC child is created: %s", obj)

        return {'pvc-name': obj.metadata.name}

Whatever is returned from any handler, is stored in the object's status
under that handler id (which is the function name by default).
We can see that with kubectl:

.. code-block:: bash

    kubectl describe evc my-claim

.. code-block:: none

    TODO

Let's add a yet another handler, but for the "update" cause.
This handler gets this stored PVC name from the creation handler,
and patches the PVC with the new size from the EVC::

    @kopf.on.update('zalando.org', 'v1', 'ephemeralvolumeclaims')
    def update_fn(spec, status, namespace, logger, **kwargs):

        size = spec.get('create_fn', {}).get('size', None)
        if not size:
            raise kopf.HandlerFatalError(f"Size must be set. Got {size!r}.")

        pvc_name = status['pvc-name']
        pvc_patch = {'spec': {'resources': {'requests': {'storage': size}}}}

        api = kubernetes.client.CoreV1Api()
        obj = api.patch_namespaced_persistent_volume_claim(
            namespace=namespace,
            name=pvc_name,
            body=pvc_patch,
        )

        logger.info(f"PVC child is updated: %s", obj)
