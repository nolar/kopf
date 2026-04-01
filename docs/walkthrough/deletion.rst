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

The straightforward way would be to implement a deletion handler
with ``@kopf.on.delete``. But we will take a different approach and use
a built-in feature of Kubernetes: `owner references`__.

__ https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/

Let us extend the creation handler:

.. code-block:: python
    :name: adopting
    :caption: ephemeral.py
    :emphasize-lines: 18

    import kopf
    import kubernetes
    import os
    import yaml
    from typing import Any

    @kopf.on.create('ephemeralvolumeclaims')
    def create_fn(spec: kopf.Spec, name: str, namespace: str | None, logger: kopf.Logger, body: kopf.Body, **_: Any) -> dict[str, str]:

        size = spec.get('size')
        if not size:
            raise kopf.PermanentError(f"Size must be set. Got {size!r}.")

        path = os.path.join(os.path.dirname(__file__), 'pvc.yaml')
        tmpl = open(path, 'rt').read()
        text = tmpl.format(name=name, size=size)
        data = yaml.safe_load(text)

        kopf.adopt(data)

        api = kubernetes.client.CoreV1Api()
        obj = api.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=data,
        )

        logger.info(f"PVC child is created: {obj}")

        return {'pvc-name': obj.metadata.name}

With this one line, :func:`kopf.adopt` marks the PVC as a child of the EVC.
This includes automatic name generation (if absent), label propagation,
namespace assignment to match the parent object's namespace,
and, finally, setting the owner reference.

The PVC is now "owned" by the EVC, meaning it has an owner reference.
When the parent EVC object is deleted,
the child PVC will also be automatically deleted by Kubernetes,
so we do not need to manage this ourselves.
