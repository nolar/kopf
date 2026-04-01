================
Results delivery
================

All handlers can return arbitrary JSON-serializable values.
Kopf then stores these values in the resource status under the name of the handler:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.create('kopfexamples')
    def create_kex_1(**_: Any) -> int:
        return 100

    @kopf.on.create('kopfexamples')
    def create_kex_2(uid: str, **_: Any) -> dict[str, int]:
        return {'r1': random.randint(0, 100), 'r2': random.randint(100, 999)}

These results are visible in the object's content:

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
the resource itself, given that handlers do not know in which order they
will be invoked (due to error handling and retrying), and to enable
recovery in case of operator failures and restarts:

.. code-block:: python

    import kopf
    import pykube
    from typing import Any

    @kopf.on.create('kopfexamples')
    def create_job(status: kopf.Status, **_: Any) -> dict[str, str]:
        if not status.get('create_pvc', {}):
            raise kopf.TemporaryError("PVC is not created yet.", delay=10)

        pvc_name = status['create_pvc']['name']

        api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        obj = pykube.Job(api, {...})  # use pvc_name here
        obj.create()
        return {'name': obj.name}

    @kopf.on.create('kopfexamples')
    def create_pvc(**_: Any) -> dict[str, str]:
        api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        obj = pykube.PersistentVolumeClaim(api, {...})
        obj.create()
        return {'name': obj.name}

.. note::

    In this example, the handlers are *intentionally* put in such an order
    that the first handler always fails on the first attempt. Having them
    in the proper order (PVC first, Job second) would make it work smoothly
    in most cases, until PVC creation fails for any temporary reason
    and has to be retried. The whole thing will eventually succeed in
    1-2 additional retries, just with less friendly messages and stack traces.
