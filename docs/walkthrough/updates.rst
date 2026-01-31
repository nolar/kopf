====================
Updating the objects
====================

.. warning::
    Unfortunately, Minikube cannot handle the PVC/PV resizing,
    as it uses the HostPath provider internally.
    You can either skip this step of the tutorial,
    or you can use an external Kubernetes cluster
    with real dynamically sized volumes.

Previously (:doc:`creation`),
we have implemented a handler for the creation of an ``EphemeralVolumeClaim`` (EVC),
and created the corresponding ``PersistantVolumeClaim`` (PVC).

What will happen if we change the size of the EVC when it already exists?
The PVC must be updated accordingly to match its parent EVC.

First, we have to remember the name of the created PVC:
Let's extend the creation handler we already have from the previous step
with one additional line:

.. code-block:: python
    :caption: ephemeral.py
    :emphasize-lines: 21

    @kopf.on.create('ephemeralvolumeclaims')
    def create_fn(spec, name, namespace, logger, **kwargs):

        size = spec.get('size')
        if not size:
            raise kopf.PermanentError(f"Size must be set. Got {size!r}.")

        path = os.path.join(os.path.dirname(__file__), 'pvc.yaml')
        tmpl = open(path, 'rt').read()
        text = tmpl.format(size=size, name=name)
        data = yaml.safe_load(text)

        api = kubernetes.client.CoreV1Api()
        obj = api.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=data,
        )

        logger.info(f"PVC child is created: {obj}")

        return {'pvc-name': obj.metadata.name}

Whatever is returned from any handler, is stored in the object's status
under that handler id (which is the function name by default).
We can see that with kubectl:

.. code-block:: bash

    kubectl get -o yaml evc my-claim

.. code-block:: yaml

    spec:
      size: 1G
    status:
      create_fn:
        pvc-name: my-claim
      kopf: {}

.. note::
    If the above change causes ``Patching failed with inconsistencies``
    debug warnings and/or your EVC YAML doesn't show a ``.status`` field,
    make sure you have set the ``x-kubernetes-preserve-unknown-fields: true``
    field in your CRD on either the entire object or just the ``.status`` field
    as detailed in :doc:`resources`.
    Without setting this field, Kubernetes will prune the ``.status`` field
    when Kopf tries to update it. For more info on field pruning,
    see `the Kubernetes docs
    <https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#field-pruning>`_.

Let's add a yet another handler, but for the "update" cause.
This handler gets this stored PVC name from the creation handler,
and patches the PVC with the new size from the EVC:

.. code-block:: python

    import kopf

    @kopf.on.update('ephemeralvolumeclaims')
    def update_fn(spec, status, namespace, logger, **kwargs):

        size = spec.get('size', None)
        if not size:
            raise kopf.PermanentError(f"Size must be set. Got {size!r}.")

        pvc_name = status['create_fn']['pvc-name']
        pvc_patch = {'spec': {'resources': {'requests': {'storage': size}}}}

        api = kubernetes.client.CoreV1Api()
        obj = api.patch_namespaced_persistent_volume_claim(
            namespace=namespace,
            name=pvc_name,
            body=pvc_patch,
        )

        logger.info(f"PVC child is updated: {obj}")

Now, let's change the EVC's size:

.. code-block:: bash

    kubectl edit evc my-claim

Or by patching it:

.. code-block:: bash

    kubectl patch evc my-claim --type merge -p '{"spec": {"size": "2G"}}'

Keep in mind the PVC size can only be increased, never decreased.

Give the operator a few seconds to handle the change.

Check the size of the actual PV behind the PVC, which is now increased:

.. code-block:: bash

    kubectl get pv

.. code-block:: none

    NAME                                       CAPACITY   ACCESS MODES   ...
    pvc-a37b65bd-8384-11e9-b857-42010a800265   2Gi        RWO            ...

.. warning::
    Kubernetes & ``kubectl`` improperly show the capacity of PVCs:
    it remains the same (1G) event after the change.
    The size of the actual PV (Persistent Volume) of each PVC is important!
    This issue is not related to Kopf, so we go around it.
