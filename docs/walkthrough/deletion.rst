=================
Cascaded deletion
=================

Previously (:doc:`creation` & :doc:`updates` & :doc:`diffs`),
we have implemented the creation of a ``PersistentVolumeClaim`` (PVC)
every time an ``EphemeralVolumeClaim`` (EVC) is created,
and cascaded updates of the size and labels when they are changed.

What will happen if the ``EphemeralVolumeClaim`` is deleted?

.. code-block:: bash

    kubectl delete evc my-claim
    kubectl delete -f evc.yaml

By default, from the Kubernetes point of view, the PVC & EVC are not connected.
Hence, the PVC will continue to exist even if its parent EVC is deleted.
Hopefully, some other controller (e.g. the garbage collector) will delete it.
Or maybe not.

We want to make sure the child PVC is deleted when the parent EVC is deleted.

The straighforward way would be to implement a deletion handler
with `kopf.on.delete`. But we will go another way, and use the
built-in feature of Kubernetes: `the owner references`__.

__ https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/

Let's extend the creation handler:

.. code-block:: python
   :name: adopting
   :linenos:
   :caption: ephemeral.py
   :emphasize-lines: 18

    import kopf
    import kubernetes
    import yaml

    @kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
    def create_fn(meta, spec, namespace, logger, **kwargs):

        name = meta.get('name')
        size = spec.get('size')
        if not size:
            raise kopf.HandlerFatalError(f"Size must be set. Got {size!r}.")

        path = os.path.join(os.path.dirname(__file__), 'pvc-tpl.yaml')
        tmpl = open(path, 'rt').read()
        text = tmpl.format(name=name, size=size)
        data = yaml.load(text)

        kopf.adopt(data, owner=body)

        api = kubernetes.client.CoreV1Api()
        obj = api.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=data,
        )

        logger.info(f"PVC child is created: %s", obj)

With this one line, `kopf.adopt` marks the PVC as child of EVC.
This includes: the name auto-generation (if absent), the label propagation,
the namespace assignment to the parent's object namespace,
and, finally, the owner referencing.

The PVC is now "owned" by the EVC, i.e. it has an owner reference.
When the parent EVC object is deleted,
the child PVC will also be deleted (and terminated in case of pods),
so that we have no need to control this ourselves.
