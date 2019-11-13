=============
Health-checks
=============

Kopf provides a minimalistic HTTP server to report its health status.


Liveness endpoints
==================

By default, no endpoint is configured, and no health is reported.
To specify an endpoint to listen for probes, use :option:`--liveness`:

.. code-block:: bash

    kopf run --liveness=http://:8080/healthz --verbose handlers.py

Currently, only HTTP is supported.
Other protocols (TCP, HTTPS) can be added in the future.


Kubernetes probing
==================

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

    Kubernetes manual on `liveness and readiness probes`__.

__ https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/

.. seealso::

    Please be aware of the readiness vs. liveness probing.
    In case of operators, readiness probing makes no practical sense,
    as operators do not serve traffic under the load balancing or with services.
    Liveness probing can help in disastrous cases (e.g. the operator is stuck),
    but will not help in case of partial failures (one of the API calls stuck).
    You can read more here:
    https://srcco.de/posts/kubernetes-liveness-probes-are-dangerous.html

.. warning::

    Make sure that one and only one pod of an operator is running at a time,
    especially during the restarts --- see :doc:`deployment`.


Probe handlers
==============

The content of the response is empty by default. It can be populated with
probing handlers:

.. code-block:: python

    import datetime
    import kopf
    import random

    @kopf.on.probe(id='now')
    def get_current_timestamp(**kwargs):
        return datetime.datetime.utcnow().isoformat()

    @kopf.on.probe(id='random')
    def get_random_value(**kwargs):
        return random.randint(0, 1_000_000)

The probe handlers will be executed on the requests to the liveness URL,
and cached for a reasonable period of time to prevent overloading
by mass-requesting the status.

The handler results will be reported as the content of the liveness response:

.. code-block:: console

    $ curl http://localhost:8080/healthz
    {"now": "2019-11-07T18:03:52.513803", "random": 765846}

.. note::
    Liveless status report is simplistic and minimalistic at the moment.
    It only reports success if the health-reporting task runs at all.
    It can happen so that some of the operator's tasks, threads, or streams
    do break, freeze, or become unresponsive, while the health-reporting task
    continues to run. The probability of such case is low, but not zero.

    There are no checks that operator actually operates anything
    (unless they are implemented explicitly with the probe-handlers),
    as there are no reliable criteria for that -- total absence of handled
    resources or events can be an expected state of the cluster.
