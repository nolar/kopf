=======
Peering
=======

All running operators communicate with each other via peering objects
(additional kind of custom resources), so they know about each other.


Priorities
==========

Each operator has a priority (the default is 0). Whenever the operator
notices that other operators start with a higher priority, it pauses
its operation until those operators stop working.

This is done to prevent collisions of multiple operators handling
the same objects. If two operators runs with the same priority  all operators
issue a warning and freeze, so that the cluster becomes not served anymore.

To set the operator's priority, use :option:`--priority`:

.. code-block:: bash

    kopf run --priority=100 ...

Or:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.peering.priority = 100

As a shortcut, there is a :option:`--dev` option, which sets
the priority to ``666``, and is intended for the development mode.


Scopes
======

There are two types of custom resources used for peering:

* ``ClusterKopfPeering`` for the cluster-scoped operators.
* ``KopfPeering`` for the namespace-scoped operators.

Kopf automatically chooses which one to use, depending on whether
the operator is restricted to a namespace with :option:`--namespace`,
or it is running cluster-wide with :option:`--all-namespaces`.

Create a peering object as needed with one of:

.. code-block:: yaml

    apiVersion: kopf.dev/v1
    kind: ClusterKopfPeering
    metadata:
      name: example

.. code-block:: yaml

    apiVersion: kopf.dev/v1
    kind: KopfPeering
    metadata:
      namespace: default
      name: example

.. note::

    In ``kopf<0.11`` (until May 2019), ``KopfPeering`` was the only CRD,
    and it was cluster-scoped. In ``kopf>=0.11,<1.29`` (until Dec 2020),
    this mode was deprecated but supported if the old CRD existed.
    Since ``kopf>=1.29`` (Jan 2021), it is not supported anymore.
    To upgrade, delete and re-create the peering CRDs to the new ones.

.. note::

    In ``kopf<1.29``, all peering CRDs used the API group ``kopf.zalando.org``.
    Since ``kopf>=1.29`` (Jan'2021), they belong to the API group ``kopf.dev``.

    At runtime, both API groups are supported. However, these resources
    of different API groups are mutually exclusive and cannot co-exist
    in the same cluster since they use the same names. Whenever possible,
    re-create them with the new API group after the operator/framework upgrade.


Custom peering
==============

The operator can be instructed to use alternative peering objects:

.. code-block:: bash

    kopf run --peering=example ...
    kopf run --peering=example --namespace=some-ns ...

Or:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.peering.name = "example"
        settings.peering.mandatory = True

Depending on :option:`--namespace` or :option:`--all-namespaces`,
either ``ClusterKopfPeering`` or ``KopfPeering`` will be used automatically.

If the peering object does not exist, the operator will pause at the start.
Using :option:`--peering` assumes that the peering is mandatory.

Please note that in the startup handler, this is not the same:
the mandatory mode must be set explicitly. Otherwise, the operator will try
to auto-detect the presence of the custom peering object, but will not pause
if it is absent -- unlike with the ``--peering=`` CLI option.

The operators from different peering objects do not see each other.

This is especially useful for the cluster-scoped operators for different
resource kinds, which should not worry about other operators for other kinds.


Standalone mode
===============

To prevent an operator from peering and talking to other operators,
the standalone mode can be enabled:

.. code-block:: bash

    kopf run --standalone ...

Or:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.peering.standalone = True

In that case, the operator will not pause if other operators with
the higher priority will start handling the objects, which may lead
to the conflicting changes and reactions from multiple operators
for the same events.


Automatic peering
=================

If there is a peering object detected with the name ``default``
(either cluster-scoped or namespace-scoped),
then it is used by default as the peering object.

Otherwise, Kopf will run the operator in the standalone mode.


Multi-pod operators
===================

Usually, one and only one operator instance should be deployed for the resource.
If that operator's pod dies, the handling of the resource of this type
will stop until the operator's pod is restarted (and if restarted at all).

To start multiple operator pods, they must be distinctly prioritised.
In that case, only one operator will be active --- the one with the highest
priority. All other operators will pause and wait until this operator exits.
Once it dies, the second-highest priority operator will come into play.
And so on.

For this, assign a monotonically growing or random priority to each
operator in the deployment or replicaset:

.. code-block:: bash

    kopf run --priority=$RANDOM ...

Or:

.. code-block:: python

    import random
    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.peering.priority = random.randint(0, 32767)

``$RANDOM`` is a feature of bash
(if you use another shell, see its man page for an equivalent).
It returns a random integer in the range 0..32767.
With high probability, 2-3 pods will get their unique priorities.

You can also use the pod's IP address in its numeric form as the priority,
or any other source of integers.


Stealth keep-alive
==================

Every few seconds (60 by default), the operator will send a keep-alive update
to the chosen peering, showing that it is still functioning. Other operators
will notice that and make decisions on their pausing or resuming.

The operator also logs a keep-alive activity to its logs. This can be
distracting. To disable:

.. code-block:: python

    import random
    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.peering.stealth = True

There is no equivalent CLI option for that.

Please note that it only affects logging. The keep-alive is sent anyway.
