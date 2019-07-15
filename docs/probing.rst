=============
Health-checks
=============

Kopf provides a minimalistic HTTP server to report its health status.

By default, no endpoint is configured, and no health is reported.
To specify an endpoint to listen for probes, use :option:`--liveness`:

.. code-block:: bash

    kopf run --liveness=http://:8080/healthz --verbose handlers.py

Currently, only HTTP is supported.
Other protocols (TCP, HTTPS) can be added in the future.

This port and path can be used in a liveness probe of the operator's deployment.
If the operator does not respond for any reason, Kubernetes will restart it.

.. code-block:: yaml

   apiVersion: apps/v1
   kind: Deployment
   spec:
     template:
       spec:
         containers:
         - name: the-only-one
           image: ...
           livenessProbe:
             httpGet:
               path: /healthz
               port: 8080

.. seealso::
    :doc:`deployment` for deployment patterns.

    Kubernetes manual on `liveness and readiness probes`__.

__ https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/

.. note::
    Liveless status report is simplistic and minimalistic at the moment.
    It only reports success if the health-reporting task runs at all.
    It can happen so that some of the operator's tasks, threads, or streams
    do break, freeze, or become unresponsive, while the health-reporting task
    continues to run. The probability of such case is low, but not zero.

    There are no checks that operator actually operates anything,
    as there are no reliable criteria for that (total absence of handled
    resources or events can be an expected state of the cluster).
