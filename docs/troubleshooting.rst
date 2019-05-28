===============
Troubleshooting
===============

.. _finalizers-blocking-deletion:

`kubectl` freezes on object deletion
====================================

This can happen if the operator is down at the moment of deletion.

The operator puts the finalizers on the objects as soon as it notices
them for the first time. When the objects are *requested for deletion*,
Kopf calls the deletion handlers and removes the finalizers,
thus releasing the object for the *actual deletion* by Kubernetes.

If the object has to be deleted without the operator starting again,
you can remove the finalizers manually:

.. code-block:: bash

    kubectl patch kopfexample kopf-example-1 -p '{"metadata": {"finalizers": []}}' --type merge

The object will be removed by Kubernetes immediately.

Alternatively, restart the operator, and allow it to remove the finalizers.
