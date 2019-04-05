======
Events
======

.. warning::
    Kubernetes itself contains a terminology conflict:
    There are *events* when watching over the objects/resources,
    such as in ``kubectl get pod --watch``.
    And there are *events* as a built-in object kind,
    as shown in ``kubectl describe pod ...`` in the "Events" section.
    In this documentation, they are distinguished as "watch-events"
    and "k8s-events". This section describes k8s-events only.

Handled objects
===============

.. todo:: the ``body`` arg must be optional, meaning the currently handled object.

Kopf provides some tools to report arbitrary information
for the handled objects as Kubernetes events::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def create_fn(body, **_):
        kopf.event(body,
                   type='SomeType',
                   reason='SomeReason',
                   message='Some message')

The type and reason are arbitrary, and can be anything.
Some restrictions apply (e.g. no spaces).
The message is also arbitrary free-text.
However, newlines are not rendered nicely
(they break the whole output of ``kubectl``).

For convenience, a few shortcuts are provided to mimic the Python's ``logging``::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def create_fn(body, **_):
        kopf.warn(body, reason='SomeReason', message='Some message')
        kopf.info(body, reason='SomeReason', message='Some message')
        try:
            raise RuntimeError("Exception text.")
        except:
            kopf.exception(body, reason="SomeReason", message="Some exception:")

These events are seen in the output of:

.. code-block:: bash

    kubectl describe kopfexample kopf-example-1

.. code-block:: none

    ...
    Events:
      Type      Reason      Age   From  Message
      ----      ------      ----  ----  -------
      Normal    SomeReason  5s    kopf  Some message
      Normal    Success     5s    kopf  Handler create_fn succeeded.
      SomeType  SomeReason  6s    kopf  Some message
      Normal    Finished    5s    kopf  All handlers succeeded.
      Error     SomeReason  5s    kopf  Some exception: Exception text.
      Warning   SomeReason  5s    kopf  Some message


Other objects
=============

.. todo:: kubernetes and pykube objects should be accepted natively, not only the dicts.

Events can be also attached to other objects, not only those handled
at the moment (and not event the children)::

    import kopf
    import kubernetes

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def create_fn(body, namespace, uid, **_):

        pod = kubernetes.client.V1Pod()
        api = kubernetes.client.CoreV1Api()
        obj = api.create_namespaced_pod(namespace, pod)

        msg = f"This pod is created by KopfExample {body['name']}"
        kopf.info(obj.to_dict(), reason='SomeReason', message=msg)

.. note::
    Events are not persistent.
    They are usually garbage-collected after some time, e.g. one hour.
    All the reported information must be only for a short-term use.
