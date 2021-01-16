=====================
Starting the operator
=====================

Previously, we have defined a :doc:`problem <problem>` that we are solving,
and created the :doc:`custom resource definitions <resources>`
for the ephemeral volume claims.

Now, we are ready to write some logic for this kind of objects.
Let's start with the an operator skeleton that does nothing useful --
just to see how it can be started.

.. code-block:: python
    :name: skeleton
    :linenos:
    :caption: ephemeral.py

    import kopf

    @kopf.on.create('ephemeralvolumeclaims')
    def create_fn(body, **kwargs):
        print(f"A handler is called with body: {body}")

.. note::
    Despite an obvious desire, do not name the file as ``operator.py``,
    since there is a built-in module in Python 3 with this name,
    and there could be potential conflicts on the imports.

Let's run the operator and see what will happen:

.. code-block:: bash

    kopf run ephemeral.py --verbose


The output looks like this:

.. code-block:: none

    [2019-05-31 10:42:11,870] kopf.config          [DEBUG   ] configured via kubeconfig file
    [2019-05-31 10:42:11,913] kopf.reactor.peering [WARNING ] Default peering object is not found, falling back to the standalone mode.
    [2019-05-31 10:42:12,037] kopf.reactor.handlin [DEBUG   ] [default/my-claim] First appearance: {'apiVersion': 'kopf.dev/v1', 'kind': 'EphemeralVolumeClaim', 'metadata': {'annotations': {'kubectl.kubernetes.io/last-applied-configuration': '{"apiVersion":"kopf.dev/v1","kind":"EphemeralVolumeClaim","metadata":{"annotations":{},"name":"my-claim","namespace":"default"}}\n'}, 'creationTimestamp': '2019-05-29T00:41:57Z', 'generation': 1, 'name': 'my-claim', 'namespace': 'default', 'resourceVersion': '47720', 'selfLink': '/apis/kopf.dev/v1/namespaces/default/ephemeralvolumeclaims/my-claim', 'uid': '904c2b9b-81aa-11e9-a202-a6e6b278a294'}}
    [2019-05-31 10:42:12,038] kopf.reactor.handlin [DEBUG   ] [default/my-claim] Adding the finalizer, thus preventing the actual deletion.
    [2019-05-31 10:42:12,038] kopf.reactor.handlin [DEBUG   ] [default/my-claim] Patching with: {'metadata': {'finalizers': ['KopfFinalizerMarker']}}
    [2019-05-31 10:42:12,165] kopf.reactor.handlin [DEBUG   ] [default/my-claim] Creation is in progress: {'apiVersion': 'kopf.dev/v1', 'kind': 'EphemeralVolumeClaim', 'metadata': {'annotations': {'kubectl.kubernetes.io/last-applied-configuration': '{"apiVersion":"kopf.dev/v1","kind":"EphemeralVolumeClaim","metadata":{"annotations":{},"name":"my-claim","namespace":"default"}}\n'}, 'creationTimestamp': '2019-05-29T00:41:57Z', 'finalizers': ['KopfFinalizerMarker'], 'generation': 1, 'name': 'my-claim', 'namespace': 'default', 'resourceVersion': '47732', 'selfLink': '/apis/kopf.dev/v1/namespaces/default/ephemeralvolumeclaims/my-claim', 'uid': '904c2b9b-81aa-11e9-a202-a6e6b278a294'}}
    A handler is called with body: {'apiVersion': 'kopf.dev/v1', 'kind': 'EphemeralVolumeClaim', 'metadata': {'annotations': {'kubectl.kubernetes.io/last-applied-configuration': '{"apiVersion":"kopf.dev/v1","kind":"EphemeralVolumeClaim","metadata":{"annotations":{},"name":"my-claim","namespace":"default"}}\n'}, 'creationTimestamp': '2019-05-29T00:41:57Z', 'finalizers': ['KopfFinalizerMarker'], 'generation': 1, 'name': 'my-claim', 'namespace': 'default', 'resourceVersion': '47732', 'selfLink': '/apis/kopf.dev/v1/namespaces/default/ephemeralvolumeclaims/my-claim', 'uid': '904c2b9b-81aa-11e9-a202-a6e6b278a294'}, 'spec': {}, 'status': {}}
    [2019-05-31 10:42:12,168] kopf.reactor.handlin [DEBUG   ] [default/my-claim] Invoking handler 'create_fn'.
    [2019-05-31 10:42:12,173] kopf.reactor.handlin [INFO    ] [default/my-claim] Handler 'create_fn' succeeded.
    [2019-05-31 10:42:12,210] kopf.reactor.handlin [INFO    ] [default/my-claim] All handlers succeeded for creation.
    [2019-05-31 10:42:12,223] kopf.reactor.handlin [DEBUG   ] [default/my-claim] Patching with: {'status': {'kopf': {'progress': None}}, 'metadata': {'annotations': {'kopf.zalando.org/last-handled-configuration': '{"apiVersion": "kopf.dev/v1", "kind": "EphemeralVolumeClaim", "metadata": {"name": "my-claim", "namespace": "default"}, "spec": {}}'}}}
    [2019-05-31 10:42:12,342] kopf.reactor.handlin [DEBUG   ] [default/my-claim] Updating is in progress: {'apiVersion': 'kopf.dev/v1', 'kind': 'EphemeralVolumeClaim', 'metadata': {'annotations': {'kopf.zalando.org/last-handled-configuration': '{"apiVersion": "kopf.dev/v1", "kind": "EphemeralVolumeClaim", "metadata": {"name": "my-claim", "namespace": "default"}, "spec": {}}', 'kubectl.kubernetes.io/last-applied-configuration': '{"apiVersion":"kopf.dev/v1","kind":"EphemeralVolumeClaim","metadata":{"annotations":{},"name":"my-claim","namespace":"default"}}\n'}, 'creationTimestamp': '2019-05-29T00:41:57Z', 'finalizers': ['KopfFinalizerMarker'], 'generation': 2, 'name': 'my-claim', 'namespace': 'default', 'resourceVersion': '47735', 'selfLink': '/apis/kopf.dev/v1/namespaces/default/ephemeralvolumeclaims/my-claim', 'uid': '904c2b9b-81aa-11e9-a202-a6e6b278a294'}, 'status': {'kopf': {}}}
    [2019-05-31 10:42:12,343] kopf.reactor.handlin [INFO    ] [default/my-claim] All handlers succeeded for update.
    [2019-05-31 10:42:12,362] kopf.reactor.handlin [DEBUG   ] [default/my-claim] Patching with: {'status': {'kopf': {'progress': None}}, 'metadata': {'annotations': {'kopf.zalando.org/last-handled-configuration': '{"apiVersion": "kopf.dev/v1", "kind": "EphemeralVolumeClaim", "metadata": {"name": "my-claim", "namespace": "default"}, "spec": {}}'}}}

Note that the operator has noticed an object created before the operator
was even started, and handled it -- since it was not handled before.

Now, you can stop the operator with Ctrl-C (twice), and start it again:

.. code-block:: bash

    kopf run ephemeral.py --verbose

The operator will not handle the object, as now it is already successfully
handled. This is important in case of the operator is restarted if it runs
in a normally deployed pod, or when you restart the operator for debugging.

Let's delete and re-create the same object to see the operator reacting:

.. code-block:: bash

    kubectl delete -f obj.yaml
    kubectl apply -f obj.yaml

