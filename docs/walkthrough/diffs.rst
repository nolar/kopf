==================
Diffing the fields
==================

Previously (:doc:`updates`), we set up cascaded updates so that
the PVC size is updated every time the EVC size changes.

What will happen if the user re-labels the EVC?

.. code-block:: bash

    kubectl label evc my-claim application=some-app owner=me

Nothing.
The EVC update handler will be called, but it only uses the size field.
Other fields are ignored.

Let us re-label the PVC with the labels of its EVC, and keep them in sync.
The sync is one-way: relabeling the child PVC does not affect the parent EVC.


Old & New
=========

It can be done the same way as the size update handlers,
but we will use another feature of Kopf to track one specific field only:

.. code-block:: python
    :name: with-new
    :caption: ephemeral.py
    :emphasize-lines: 1, 5

    @kopf.on.field('ephemeralvolumeclaims', field='metadata.labels')
    def relabel(old: Any, new: Any, status: kopf.Status, namespace: str | None, **_: Any) -> None:

        pvc_name = status['create_fn']['pvc-name']
        pvc_patch = {'metadata': {'labels': new}}

        api = kubernetes.client.CoreV1Api()
        obj = api.patch_namespaced_persistent_volume_claim(
            namespace=namespace,
            name=pvc_name,
            body=pvc_patch,
        )

The :kwarg:`old` & :kwarg:`new` kwargs contain the old & new values of the field
(or of the whole object for the object handlers).

It will work as expected when the user adds new labels and changes the existing
labels, but not when the user deletes the labels from the EVC.

Why? Because of how patching works in Kubernetes API:
it *merges* the dictionaries (with some exceptions).
To delete a field from the object, you need to set it to ``None``
in the patch object.

So, we need to know which fields were deleted from the EVC.
Kubernetes does not natively provide this information in object events,
since it notifies operators only with the latest state of the object ---
as seen in the :kwarg:`body`/:kwarg:`meta` kwargs.


Diffs
=====

Kopf tracks the state of the objects and calculates the diffs.
The diffs are provided as the :kwarg:`diff` kwarg; the old & new states
of the object or field --- as the :kwarg:`old` & :kwarg:`new` kwargs.

A diff-object has this structure:

.. code-block:: python

    ((action, n-tuple of object or field path, old, new),)

with example:

.. code-block:: python

    (('add', ('metadata', 'labels', 'label1'), None, 'new-value'),
     ('change', ('metadata', 'labels', 'label2'), 'old-value', 'new-value'),
     ('remove', ('metadata', 'labels', 'label3'), 'old-value', None),
     ('change', ('spec', 'size'), '1G', '2G'))

For the field-handlers, it will be the same,
but the field path will be relative to the handled field,
and unrelated fields will be filtered out.
For example, if the field is ``metadata.labels``:

.. code-block:: python

    (('add', ('label1',), None, 'new-value'),
     ('change', ('label2',), 'old-value', 'new-value'),
     ('remove', ('label3',), 'old-value', None))

Now, let us use this feature to explicitly react to the relabeling of the EVCs.
Note that the ``new`` value for a removed dict key is ``None``,
which is exactly what the patch object needs to delete that field:

.. code-block:: python
    :name: with-diff
    :caption: ephemeral.py
    :emphasize-lines: 4

    @kopf.on.field('ephemeralvolumeclaims', field='metadata.labels')
    def relabel(diff: kopf.Diff, status: kopf.Status, namespace: str | None, **_: Any) -> None:

        labels_patch = {field[0]: new for op, field, old, new in diff}
        pvc_name = status['create_fn']['pvc-name']
        pvc_patch = {'metadata': {'labels': labels_patch}}

        api = kubernetes.client.CoreV1Api()
        obj = api.patch_namespaced_persistent_volume_claim(
            namespace=namespace,
            name=pvc_name,
            body=pvc_patch,
        )

Note that unrelated labels placed on the PVC --- e.g. manually,
from a template, or by other controllers/operators, besides the labels
coming from the parent EVC --- are preserved and never touched
(unless a label with the same name is applied to the EVC and propagated to the PVC).

.. code-block:: bash

    kubectl describe pvc my-claim

.. code-block:: none

    Name:          my-claim
    Namespace:     default
    StorageClass:  standard
    Status:        Bound
    Labels:        application=some-app
                   owner=me
