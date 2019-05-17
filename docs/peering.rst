=======
Peering
=======

All running operators communicate with each other via peering objects
(additional kind of custom resources), so they know about each other.


Priorities
==========

Each operator has a priority (the default is 0). Whenever the operator
notices that other operators start with a higher priority, it freezes
its operation until those operators stop working.

This is done to prevent collisions of multiple operators handling
the same objects.

To set the operator's priority, use :option:`--priority`:

.. code-block:: bash

    kopf run --priority=100 ...

As a shortcut, there is a :option:`--dev` option, which sets
the priority to ``666``, and is intended for the development mode.


Scopes
======

There are two types of custom resources used for peering:

* ``ClusterKopfPeering`` for the cluster-scoped operators.
* ``KopfPeering`` for the namespace-scoped operators.

Kopf automatically chooses which one to use, depending on whether
the operator is restricted to a namespace with :option:`--namespace`,
or it is running unrestricted and cluster-wide.

Create the peering objects as needed with one of:

.. code-block:: yaml

    apiVersion: zalando.org/v1
    kind: ClusterKopfPeering
    metadata:
      name: example

.. code-block:: yaml

    apiVersion: zalando.org/v1
    kind: KopfPeering
    metadata:
      namespace: default
      name: example


.. note::

    Previously, ``KopfPeering`` was the only CRD, and it was cluster-scoped.
    Now, it is namespaced. For the new users, it all will be fine and working.

    If the old ``KopfPeering`` CRD is already deployed to your cluster,
    it will also continue to work as before without re-configuration:
    though there will be no namespace isolation as documented here ---
    it will be cluster peering regardless of :option:`--namespace`
    (as it was before the changes).

    When possible (but strictly after the Kopf's version upgrade),
    please delete the old CRD, and re-create from scratch:

    .. code-block:: bash

        kubectl delete crd kopfpeerings.zalando.org
        # give it 1-2 minutes to cleanup, or repeat until succeeded:
        kubectl create -f peering.yaml

    Then re-deploy your custom peering objects of your apps.


Custom peering
==============

The operator can be instructed to use alternative peering objects::

    kopf run --peering=example ...
    kopf run --peering=example --namespace=some-ns ...

Depending on :option:`--namespace`, either ``ClusterKopfPeering``
or ``KopfPeering`` will be used (in the operator's namespace).

If the peering object does not exist, the operator will fail to start.
Using :option:`--peering` assumes that the peering is required.

The operators from different peering objects do not see each other.

This is especially useful for the cluster-scoped operators for different
resource kinds, which should not worry about other operators for other kinds.


Standalone mode
===============

To prevent an operator from peering and talking to other operators,
the standalone mode can be enabled::

    kopf run --standalone ...

In that case, the operator will not freeze if other operators with
the higher priority will start handling the objects, which may lead
to the conflicting changes and reactions from multiple operators
for the same events.


Automatic peering
=================

If there is a peering object detected with name `default` (either
cluster-scoped or namespace-scoped, depending on :option:`--namespace`),
then it is used by default as the peering object.

Otherwise, Kopf will issue a warning and will run the operator
in the standalone mode.


Multi-pod operators
===================

Usually, one and only one operator instance should be deployed for the resource.
If that operator's pod dies, the handling of the resource of this type
will stop until the operator's pod is restarted (and if restarted at all).

To start multiple operator pods, they must be distinctly prioritised.
In that case, only one operator will be active --- the one with the highest
priority. All other operators will freeze and wait until this operator dies.
Once it dies, the second highest priority operator will come into play.
And so on.

For this, assign a monotonically growing or random priority to each
operator in the deployment or replicaset:

.. code-block:: bash

    kopf run --priority=$RANDOM ...

``$RANDOM`` is a feature of bash
(if you use another shell, see its man page for an equivalent).
It returns a random integer in the range 0..32767.
With high probability, 2-3 pods will get their unique priorities.

You can also use the pod's IP address in its numeric form as the priority,
or any other source of integers.
