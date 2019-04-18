==================
Diffing the fields
==================

Previously (:doc:`updates`), we have set the size of PVC to be updated
every time the size of EVC is updated, i.e. the cascaded updates.

What will happen if the user re-labels the EVC?

.. code-block:: bash

    kubectl label evc my-claim application=some-app owner=me

Nothing.
The EVC update handler will be called, but it only uses the size field.
Other fields are ignored.

Let's re-label the PVC with the labels of its EVC, and keep them in sync.
The sync is one-way: re-labelling the child PVC does not affect the parent EVC.


Old & New
=========

It can be done the same way as the size update handlers,
but we will use another feature of Kopf to track one specific field only:

.. code-block:: python
    :name: with-new
    :linenos:
    :caption: ephemeral.py
    :emphasize-lines: 1, 5

    @kopf.on.field('zalando.org', 'v1', 'ephemeralvolumeclaims', field='metadata.labels')
    def relabel(old, new, **kwargs):

        pvc_name = status['pvc-name']
        pvc_patch = {'metadata': {'labels': new}}

        api = kubernetes.client.CoreV1Api()
        obj = api.patch_namespaced_persistent_volume_claim(
            namespace=namespace,
            name=pvc_name,
            body=pvc_patch,
        )

The ``old`` & ``new`` kwargs contain the old & new values of the field
(or of the whole object for the object handlers).

It will work as expected when the user adds new labels and changes the existing
labels, but not when the user deletes the labels from the EVC.

Why? Because of how patching works in Kubernetes API:
it *merges* the dictionaries (with some exceptions).
To delete a field from the object, it should be set to ``None``
in the patch object.

So, we should know which fields were deleted from EVC.
Natively, Kubernetes does not provide this information for the object events,
since Kubernetes notifies the operators only with the newest state of the object
-- as seen in ``body``/``metadata`` kwargs.


Diffs
=====

Kopf tracks the state of the objects, and calculates the diffs.
The diffs are provided as the ``diff`` kwarg; the old & new states
of the object or field -- as the ``old`` & ``new`` kwargs.

A diff-object has this structure (as an example)::

    [('add', ('metadata', 'labels', 'label1'), None, 'new-value'),
     ('change', ('metadata', 'labels', 'label2'), 'old-value', 'new-value'),
     ('remove', ('metadata', 'labels', 'label3'), 'old-value', None),
     ('change', ('spec', 'size'), '10G', '100G')]

For the field-handlers, it will be the same,
just the field path will be relative to the handled field,
and unrelated fields will be filtered out.
For example, if the field is ``metadata.labels``::

    [('add', ('label1',), None, 'new-value'),
     ('change', ('label2',), 'old-value', 'new-value'),
     ('remove', ('label3',), 'old-value', None)]

Now, let's use this feature to explicitly react to re-labelling of the EVCs.
Note that the ``new`` value for the removed dict key is ``None``,
exactly as needed for the patch object (i.e. the field is present there):

.. code-block:: python
    :name: with-diff
    :linenos:
    :caption: ephemeral.py
    :emphasize-lines: 4

    @kopf.on.field('zalando.org', 'v1', 'ephemeralvolumeclaims', field='metadata.labels')
    def relabel(diff, **kwargs):

        labels_patch = {field[0]: new for op, field, old, new in diff}
        pvc_name = status['pvc-name']
        pvc_patch = {'metadata': {'labels': labels_patch}}

        api = kubernetes.client.CoreV1Api()
        obj = api.patch_namespaced_persistent_volume_claim(
            namespace=namespace,
            name=pvc_name,
            body=pvc_patch,
        )

Note that the unrelated labels that were put on the PVC ---e.g., manually,
from the template, by other controllers/operators, beside the labels
coming from the parent EVC--- are persisted and never touched
(unless the same-named label is applied to EVC and propagated to the PVC).
