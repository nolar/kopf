================
Results delivery
================

All handlers can return arbitrary JSON-serializable values.
These values are then put to the resource status under the name of the handler:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples')
    def create_kex_1(**_):
        return 100

    @kopf.on.create('kopfexamples')
    def create_kex_2(uid, **_):
        return {'r1': random.randint(0, 100), 'r2': random.randint(100, 999)}

These results can be seen in the object's content:

.. code-block:: console

    $ kubectl get -o yaml kex kopf-example-1

.. code-block:: none

    ...
    status:
      create_kex_1: 100
      create_kex_2:
        r1: 66
        r2: 666

The function results can be used to communicate between handlers through
resource itself, assuming that handlers do not know in which order they
will be invoked (due to error handling and retrying), and to be able to
restore in case of operator failures & restarts:

.. code-block:: python

    import kopf
    import pykube

    @kopf.on.create('kopfexamples')
    def create_job(status, **_):
        if not status.get('create_pvc', {}):
            raise kopf.TemporaryError("PVC is not created yet.", delay=10)

        pvc_name = status['create_pvc']['name']

        api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        obj = pykube.Job(api, {...})  # use pvc_name here
        obj.create()
        return {'name': obj.name}

    @kopf.on.create('kopfexamples')
    def create_pvc(**_):
        api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        obj = pykube.PersistentVolumeClaim(api, {...})
        obj.create()
        return {'name': obj.name}

.. note::

    In this example, the handlers are *intentionally* put in such an order
    that the first handler always fails on the first attempt. Having them
    in the proper order (PVC first, Job afterwards) will make it work smoothly
    for most of the cases, until PVC creation fails for any temporary reason
    and has to be retried. The whole thing will eventually succeed anyway in
    1-2 additional retries, just with less friendly messages and stack traces.
